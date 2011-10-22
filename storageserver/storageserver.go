package storageserver

import (
	"storageproto"
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
	return nil
}

func (ss *Storageserver) GetListRPC(args *storageproto.GetArgs, reply *storageproto.GetListReply) os.Error {
	return nil
}

func (ss *Storageserver) PutRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	return nil
}

func (ss *Storageserver) AppendToListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	return nil
}

func (ss *Storageserver) RemoveFromListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	return nil
}


func (ss *Storageserver) GET(key string) ([]byte, bool) {
}

func (ss *StorageServer) PUT(key string, val []byte) (bool) {
}

func (ss *StorageServer) AddToList(key string, element []byte) (os.Error) {
}

func (ss *StorageServer) RemoveFromList(key string, element []byte) (os.Error) {
}
