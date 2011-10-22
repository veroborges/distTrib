package main

// This file is part of handout 2.  It is NOT intended to replace
// your original server.go.  Rather, you should take the main
// function, new flags, and new argument to NewTribserver from
// this, but leave your existing tribble server code.

import (
	// Your import statements here...
)


type Tribserver struct {
	ss *storageserver.Storageserver
}

func NewTribserver(ss *storageserver.Storageserver) *Tribserver {
	return &Tribserver{ss}
}



var portnum *int = flag.Int("port", 9009, "port # to listen on")
var storageMasterNodePort *string = flag.String("master", "localhost:9009", "Storage master node")
var numNodes *int = flag.Int("N", 0, "Become the master.  Specifies the number of nodes in the system, including the master")
var nodeID *uint = flag.Uint("id", 0, "The node ID to use for consistent hashing.  Should be a 32 bit number.")

func main() {
	flag.Parse()
	log.Printf("Server starting on port %d\n", *portnum);
	ss := storageserver.NewStorageserver(*storageMasterNodePort, *numNodes, *portnum, uint32(*nodeID))
	ts := NewTribserver(ss)
	rpc.Register(ts)
	srpc := storagerpc.NewStorageRPC(ss)
	rpc.Register(srpc)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}