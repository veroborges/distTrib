package main

import (
	"fmt"
	"flag"
	"storageproto"
	"strings"
	"log"
	"rpc"
	"net"
)


// For parsing the command line
type cmd_info struct {
	cmdline string
	funcname string
	nargs int // number of required args
}
const (
	CMD_PUT = iota
	CMD_GET
)

var portnum *int = flag.Int("port", 9009, "server port # to connect to")
var serverAddress *string = flag.String("host", "localhost", "server host to connect to")


func main() {
	flag.Parse()
	if (flag.NArg() < 2) {
		log.Fatal("Insufficient arguments to client")
	}

	cmd := flag.Arg(0)
	
	serverPort := fmt.Sprintf("%d", *portnum)
	client, err := rpc.DialHTTP("tcp", net.JoinHostPort(*serverAddress, serverPort))
	if (err != nil) {
		log.Fatal("Could not connect to server:", err)
	}

	cmdlist := []cmd_info {
		{ "p", "StorageRPC.Put", 2 },
		{ "g", "StorageRPC.Get", 1 },
		{ "la", "StorageRPC.AppendToList", 2 },
		{ "lr", "StorageRPC.RemoveFromList", 2 },
		{ "lg", "StorageRPC.GetList", 1 },
	}

	cmdmap := make(map[string]cmd_info)
	for _, j := range(cmdlist) {
		cmdmap[j.cmdline] = j
	}

	ci, found := cmdmap[cmd]
	if (!found) {
		log.Fatal("Unknown command ", cmd)
	}
	if (flag.NArg() < (ci.nargs+1)) {
		log.Fatal("Insufficient arguments for ", cmd)
	}

	// This is a little ugly, but it's quick to code. :)
	// What's the saying?  "Do what I say, not what I do."
	var putargs *storageproto.PutArgs
	getargs := &storageproto.GetArgs{flag.Arg(1)}
	getreply := &storageproto.GetReply{}
	putreply := &storageproto.PutReply{}
	getlistreply := &storageproto.GetListReply{}
	if (ci.nargs == 2) {
		putargs = &storageproto.PutArgs{flag.Arg(1), flag.Arg(2)}
	}
	var status int
	switch(cmd) {
	case "g":
		err = client.Call(ci.funcname, getargs, getreply)
		status = getreply.Status
	case "lg":
		err = client.Call(ci.funcname, getargs, getlistreply)
		status = getlistreply.Status
	case "p", "la", "lr":
		err = client.Call(ci.funcname, putargs, putreply)
		status = putreply.Status
	}
	if (err != nil) { 
		fmt.Println(ci.funcname, " failed: ", err)
	} else if (status != storageproto.OK) {
		fmt.Print("error\t", flag.Arg(1), "\t")
		switch(status) {
		case storageproto.EKEYNOTFOUND:
			fmt.Println("key not found")
		case storageproto.EITEMNOTFOUND:
			fmt.Println("item not found")
		case storageproto.EPUTFAILED:
			fmt.Println("put failed")
		case storageproto.EITEMEXISTS:
			fmt.Println("Item already exists in list")
		}
	} else {
		switch(cmd) {
		case "g":
			fmt.Println(flag.Arg(1), "\t", getreply.Value)
		case "lg":
			fmt.Println(flag.Arg(1), "\t", strings.Join(getlistreply.Value, "\t"))
		case "p", "la", "lr":
			fmt.Println(ci.funcname, " succeeded")
		}
	}
}
