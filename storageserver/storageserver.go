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
)

type Storageserver struct {
	tm *storage.TribMap
	servers []serverData
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
	ss.cond = sync.NewCond(lock)
	socket := net.JoinHostPort(master, fmt.Sprintf("%d", portnum))
	if (numnodes == 0) {
		for {
			master, err := rpc.DialHTTP("tcp", socket)
			if err != nil {
				log.Printf("Failed to connect to the master; retrying")
				time.Sleep(1000000000)
			} else {
				reply := new(storageproto.RegisterReply)
				err = master.Call("StorageRPC.Register",&RegisterArgs{Client{socket, nodeid}}, reply)
				if (err != nil) {
					log.Printf("Something went wrong with the call; retrying")
				} else if reply.Ready {
					ss.servers = make([]serverData, serverlen(reply.Clients))
					for i:=0; i < len(reply.Clients); i++ {
						ss.servers[i] = &serverData{nil, reply.Clients[i]}
					}
					break
				}
				time.Sleep(1000000000)
			}
		}
	} else {
		ss.servers = make([]serverData, numnodes)
		ss.cond.L.Lock()
		for numnodes != 0 {
			ss.cond.Wait()
		}
		ss.cond.Unlock()
	}
	for i:=0; i < len(reply.Clients); i++ {
		ss.servers[i].rpc, err := rpc.DialHTTP("tcp", ss.servers[i].clientInfo.HostPort)
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
	if (numnodes > 0) {
		ss.servers[numnodes - 1].clientInfo = args.ClientInfo
		numnodes--
	}
	reply.Ready = false
	if (numnodes == 0) {
		ss.cond.Broadcast()
		reply.Ready = true
		for i:=0; i < len(ss.servers); i++ {
			reply.Clients[i] = ss.servers[i].clientInfo
		}
	}
	ss.cond.Unlock()
	return nil
}

func (ss *Storageserver) GetRPC(args *storageproto.GetArgs, reply *storageproto.GetReply) os.Error {
	val, ok := ss.tm.GET(args.Key)
	if (!ok) {
		reply.Status = storageproto.EKEYNOTFOUND
	} else {
		reply.Status = storageproto.OK
		reply.Value = string(val)
	}
	return nil
}

func (ss *Storageserver) GetListRPC(args *storageproto.GetArgs, reply *storageproto.GetListReply) os.Error {
	val, ok := ss.tm.GET(args.Key)
	if (!ok) {
		reply.Status = storageproto.EKEYNOTFOUND
	} else {
		set := make(map[string] bool)
		err := json.Unmarshal(val, &set)
		if (err != nil) {
			reply.Status = EKEYNOTFOUND
		} else {
			reply.Value = make([]string, len(set))
			i:=0
			for k, _ = range set {
				reply.Value[i]=k
				i++
			}
			reply.Status = OK
		}

	}
	return nil
}

func (ss *Storageserver) PutRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	ss.tm.PUT(args.Key, []byte(args.Value))
	reply.Status = storageproto.OK
}

func (ss *Storageserver) AppendToListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	err := ss.tm.AddToList(args.Key, byte[](args.Value))
	if (err == nil) {
		reply.Status = storageproto.OK
	} else {
		reply.Status = storageproto.EPUTFAILED
	}
	return nil
}

func (ss *Storageserver) RemoveFromListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	err, ok := ss.tm.RemoveFromList(args.Key, byte[](args.Value))
	if (err != nil || !ok) {
		reply.Status = storageproto.EITEMNOTFOUND
	} else {
		reply.Status = storageproto.OK
	}
}

func (ss *Storageserver) GET(key string) ([]byte, int, os.Error) {
  index := ss.GetIndex(key)
	serv := ss.servers[index]

	//if local server call server tribmap
	if serv.nodeid == ss.nodeid {
		return ss.tm.GET(key), nil //can i do this? 

	}else{
		//if not, call correct server via rpc
		args := &storageproto.GetArgs{key}
		var reply tribproto.GetReply

		err := serv.server.Call("StorageRPC.Get", args, &reply)
		if (err != nil) {
				return nil, 0, err //is this the correct status for when err?
		}
		return []byte(reply.Value), reply.Status, nil
	  }
}

func (ss *Storageserver) PUT(key string, val []byte) (bool, os.Error) {
	index := ss.GetIndex(key)
	serv := ss.servers[index]

	//if local server call server tribmap
	if serv.nodeid == ss.nodeid {
		return ss.tm.PUT(key, val), nil
	}else{
		//if not, call correct server via rpc
		args := &storageproto.PutArgs{key, string(val)}
		var reply tribproto.PutReply
		err := serv.server.Call("StorageRPC.Put", args, &reply)
		if (err != nil) {
		  return 0, err
	  }
		return reply.Status, nil
	}
}

func (ss *Storageserver) AddToList(key string, element []byte) (bool, os.Error) {
	index := ss.GetIndex(key)
	serv := ss.servers[index]

	//if local server call server tribmap
	if serv.nodeid == ss.nodeid {
		return ss.tm.AddToList(key, element), nil
	}else{
		//if not, call correct server via rpc
		args := &storageproto.PutArgs{key, string(element)}
		var reply tribproto.PutReply
		err := serv.server.Call("StorageRPC.AppendToList", args, &reply)
		if (err != nil) {
		  return 0, err
	  }
		return reply.Status, nil
	}
}

func (ss *Storageserver) RemoveFromList(key string, element []byte) (bool, os.Error) {
	index := ss.GetIndex(key)
	serv := ss.servers[index]

	//if local server call server tribmap
	if serv.nodeid == ss.nodeid {
		return ss.tm.RemoveFromList(key, element), nil
	}else{
		//if not, call correct server via rpc
		args := &storageproto.PutArgs{key, string(value)}
		var reply tribproto.PutReply

		err := serv.server.Call("StorageRPC.RemoveFromList", args, &reply)
		if (err != nil) {
		  return 0, err
	  }
		return reply.Status, nil
	}
}

func (ss *Storageserver) GetIndex(key string) (int) {
	fields := strings.Split(key, ":")
	user := fields[0]
	h = New32()
	h.Write([]byte(user))

	return (h.Sum32() % len(servers)) - 1
}

