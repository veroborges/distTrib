// This is just a hackish little adapter so we can have a mix of RPC-able and
// locally callable exported interfaces from the storageserver.
//
// Please do not modify this file for 15-440.
// Implement your changes inside the storageserver implementation instead.
//

package storagerpc

import (
	"storageproto"
	"storageserver"
	"os"
)

type StorageRPC struct {
	ss *storageserver.Storageserver
}

func NewStorageRPC(ss *storageserver.Storageserver) *StorageRPC {
	return &StorageRPC{ss}
}

func (srpc *StorageRPC) Get(args *storageproto.GetArgs, reply *storageproto.GetReply) os.Error {
	return srpc.ss.GetRPC(args, reply)
}

func (srpc *StorageRPC) GetList(args *storageproto.GetArgs, reply *storageproto.GetListReply) os.Error {
	return srpc.ss.GetListRPC(args, reply)
}

func (srpc *StorageRPC) Put(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	return srpc.ss.PutRPC(args, reply)
}

func (srpc *StorageRPC) AppendToList(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	return srpc.ss.AppendToListRPC(args, reply)
}

func (srpc *StorageRPC) RemoveFromList(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	return srpc.ss.RemoveFromListRPC(args, reply)
}

func (srpc *StorageRPC) Register(args *storageproto.RegisterArgs, reply *storageproto.RegisterReply) os.Error {
	return srpc.ss.RegisterRPC(args, reply)
}
