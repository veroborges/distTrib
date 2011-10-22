package storageserver

import (
	"storageproto"
	"hash/fnv"
	"strings"
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

