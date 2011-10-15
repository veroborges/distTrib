package main

import (
	"rpc"
	"http"
	"net"
	"os"
	"log"
	"fmt"
	"tribproto"
	"flag"
  "json"
	"time"
	"sync"
)

//Global Params


//map of userid -> user struct
var users = map[string] *User 

//global tribble map
var tribbles *TribMap

//map that includes go mutex
type TribMap struct {
	data map[string] string  //tribble id("usr:tribCounter")  -> tribble struct
	theLock sync.RWMutex
}

type User struct {
	suscribers map [string] bool
	counter int
	tribbles [int] bool
}

type Tribserver struct {
}

func NewTribserver() *Tribserver {
	ts := new(Tribserver)
	return ts
}

func (ts *Tribserver) GET(key string) string { 
	
}

func (ts *Tribserver) PUT(key string, value string){

}

func (ts *Tribserver) AddToList(key string, value string){
				
}

func (ts *Tribserver) RemoveFromList(key string, value string){


}

func NewServMap() *ServMap {
	return &ServMap {
		data: make(map [string] string)
	}
} 

func (ts *Tribserver) CreateUser(args *tribproto.CreateUserArgs, reply *tribproto.CreateUserReply) os.Error {
	// Set responses by modifying the reply structure, like:
	// reply.Status = tribproto.EEXISTS
	// Note that OK is the default;  you don't need to set it explicitly
	return nil
}

func (ts *Tribserver) AddSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) os.Error {
	return nil
}

func (ts *Tribserver) RemoveSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) os.Error {
	return nil
}

func (ts *Tribserver) GetSubscriptions(args *tribproto.GetSubscriptionsArgs, reply *tribproto.GetSubscriptionsReply) os.Error {
	return nil
}

func (ts *Tribserver) PostTribble(args *tribproto.PostTribbleArgs, reply *tribproto.PostTribbleReply) os.Error {
	return nil
}

func (ts *Tribserver) GetTribbles(args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) os.Error {
	return nil
}

func (ts *Tribserver) GetTribblesBySubscription(args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) os.Error {
	return nil
}


var portnum *int = flag.Int("port", 9009, "port # to listen on")

func main() {
	flag.Parse()
	log.Printf("Server starting on port %d\n", *portnum);
	ts := NewTribserver()
	rpc.Register(ts)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
