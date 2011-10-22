package storageserver

import (
	"storageproto"
	"rpc"
	"storage"
	"storagerpc"
	"os"
	"time"
)

type Storageserver struct {
	tm *TribMap
	servers []*serverData
	numnodes int
}

type serverData {
	server *rpc.Client
	nodeid uint32
}

func NewStorageserver(master string, numnodes int, portnum int, nodeid uint32) *Storageserver {
	ss := &Storageserver{storage.NewTribMap(), make([]*rpc.Client, 0, 0), numnodes}
	if (numnodes == 0) {
		for {
			master, err := rpc.DialHTTP("tcp", net.JoinHostPort(master, portnum))
			if err != nil {
				log.Info("Failed to connect to the master; retrying")
				time.Sleep(1000000000)
			} else {
				reply := new(storageproto.RegisterReply)
				master.Call("StorageRPC.Register", 
			}
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
