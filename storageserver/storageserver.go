package storageserver

import (
	"storageproto"
	"hash/fnv"
	"strings"
	"rpc"
	"storage"
	"os"
	"time"
	"sync"
	"net"
	"log"
	"fmt"
	"json"
)

type Storageserver struct {
	tm *storage.TribMap
	servers []*serverData
	numnodes int
	nodeid uint32
	cond *sync.Cond
}

type serverData struct {
	rpc *rpc.Client
	clientInfo storageproto.Client
}

func NewStorageserver(master string, numnodes int, portnum int, nodeid uint32) *Storageserver {
	ss := &Storageserver{storage.NewTribMap(), nil, numnodes, nodeid, nil}
	var lock sync.Mutex
	ss.cond = sync.NewCond(&lock)
	socket := net.JoinHostPort(master, fmt.Sprintf("%d", portnum))
	if (numnodes == 0) {
		for {
			master, err := rpc.DialHTTP("tcp", socket)
			if err != nil {
				log.Printf("Failed to connect to the master; retrying")
				time.Sleep(1000000000)
			} else {
				reply := new(storageproto.RegisterReply)
				err = master.Call("StorageRPC.Register",&storageproto.RegisterArgs{storageproto.Client{socket, nodeid}}, reply)
				if (err != nil) {
					log.Printf("Something went wrong with the call; retrying")
				} else if reply.Ready {
						ss.servers = make([]*serverData, len(reply.Clients))
						for i:=0; i < len(reply.Clients); i++ {
							ss.servers[i] = &serverData{nil, reply.Clients[i]}
						}
					break
				}
				time.Sleep(1000000000)
			}
		}
	} else {
		ss.servers = make([]*serverData, numnodes)
		ss.cond.L.Lock()
		for numnodes != 0 {
			ss.cond.Wait()
		}
		ss.cond.L.Unlock()
	}
	for i:=0; i < len(ss.servers); i++ {
		var err os.Error
		ss.servers[i].rpc, err = rpc.DialHTTP("tcp", ss.servers[i].clientInfo.HostPort)
		if err != nil {
			log.Fatal("Problem creating rpc connection to %s", ss.servers[i].clientInfo.HostPort)
		}
	}
	return ss
}

// You might define here the functions that the locally-linked application
// logic can use to call things like Get, GetList, Put, etc.
// OR, you can have them access the local storage node
// by calling the RPC functions.  Your choice!


// RPC-able interfaces, bridged via StorageRPC.
// These should do something! :-)

func (ss *Storageserver) RegisterRPC(args *storageproto.RegisterArgs, reply *storageproto.RegisterReply) os.Error {
	ss.cond.L.Lock()
	if (ss.numnodes > 0) {
		ss.servers[ss.numnodes - 1].clientInfo = args.ClientInfo
		ss.numnodes--
	}
	reply.Ready = false
	if (ss.numnodes == 0) {
		ss.cond.Broadcast()
		reply.Ready = true
		for i:=0; i < len(ss.servers); i++ {
			reply.Clients[i] = ss.servers[i].clientInfo
		}
	}
	ss.cond.L.Unlock()
	return nil
}

func (ss *Storageserver) GetRPC(args *storageproto.GetArgs, reply *storageproto.GetReply) os.Error {
	val, status := ss.tm.GET(args.Key)
	reply.Status = status
	if (status == storageproto.OK) {
		reply.Value = string(val)
	}
	return nil
}

func (ss *Storageserver) GetListRPC(args *storageproto.GetArgs, reply *storageproto.GetListReply) os.Error {
	val, status := ss.tm.GET(args.Key)
	reply.Status = status
	if (status == storageproto.OK) {
		set := make(map[string] bool)
		err := json.Unmarshal(val, &set)
		if (err != nil) {
			reply.Status = storageproto.EPUTFAILED
		} else {
			reply.Value = make([]string, len(set))
			i:=0
			for key, _ := range set {
				reply.Value[i]=key
				i++
			}
		}
	}
	return nil
}

func (ss *Storageserver) PutRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	status := ss.tm.PUT(args.Key, []byte(args.Value))
	reply.Status = status
	return nil
}

func (ss *Storageserver) AppendToListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	status, err := ss.tm.AddToList(args.Key, []byte(args.Value))
	reply.Status = status
	return err
}

func (ss *Storageserver) RemoveFromListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	status, err := ss.tm.RemoveFromList(args.Key, []byte(args.Value))
	reply.Status = status
	return err
}

func (ss *Storageserver) GET(key string) ([]byte, int, os.Error) {
  index := ss.GetIndex(key)
	serv := ss.servers[index]

	//if local server call server tribmap
	if serv.clientInfo.NodeID == ss.nodeid {
		res, stat := ss.tm.GET(key)
		return res, stat, nil
	}
	//if not, call correct server via rpc
	args := &storageproto.GetArgs{key}
	var reply storageproto.GetReply
	err := serv.rpc.Call("StorageRPC.Get", args, &reply)
	if (err != nil) {
		return nil, 0, err
	}
	return []byte(reply.Value), reply.Status, nil
}

func (ss *Storageserver) PUT(key string, val []byte) (int, os.Error) {
	index := ss.GetIndex(key)
	serv := ss.servers[index]

	//if local server call server tribmap
	if serv.clientInfo.NodeID == ss.nodeid {
		return ss.tm.PUT(key, val), nil
	}
	//if not, call correct server via rpc
	args := &storageproto.PutArgs{key, string(val)}
	var reply storageproto.PutReply
	err := serv.rpc.Call("StorageRPC.Put", args, &reply)
	if (err != nil) {
		return 0, err
	}
	return reply.Status, nil
}

func (ss *Storageserver) AddToList(key string, element []byte) (int, os.Error) {
	index := ss.GetIndex(key)
	serv := ss.servers[index]

	//if local server call server tribmap
	if serv.clientInfo.NodeID == ss.nodeid {
		return ss.tm.AddToList(key, element)
	}
	//if not, call correct server via rpc
	args := &storageproto.PutArgs{key, string(element)}
	var reply storageproto.PutReply
	err := serv.rpc.Call("StorageRPC.AppendToList", args, &reply)
	if (err != nil) {
		return 0, err
	}
	return reply.Status, nil
}

func (ss *Storageserver) RemoveFromList(key string, element []byte) (int, os.Error) {
	index := ss.GetIndex(key)
	serv := ss.servers[index]

	//if local server call server tribmap
	if serv.clientInfo.NodeID == ss.nodeid {
		return ss.tm.RemoveFromList(key, element)
	}
	//if not, call correct server via rpc
	args := &storageproto.PutArgs{key, string(element)}
	var reply storageproto.PutReply
	err := serv.rpc.Call("StorageRPC.RemoveFromList", args, &reply)
	if (err != nil) {
		return 0, err
	}
	return reply.Status, nil
}

func (ss *Storageserver) GetIndex(key string) (int) {
	fields := strings.Split(key, ":")
	user := fields[0]
	h := fnv.New32()
	h.Write([]byte(user))
	return (int(h.Sum32()) % len(ss.servers)) - 1
}

