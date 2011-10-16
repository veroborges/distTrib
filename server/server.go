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


//tribServer Struct


type Tribserver struct {
}

func NewTribserver() *Tribserver {
	ts := new(Tribserver)
	return ts
}

} 

func (ts *Tribserver) CreateUser(args *tribproto.CreateUserArgs, reply *tribproto.CreateUserReply) os.Error {
	
	//if a map fot that user exists (present is true) return EEXISTS
	if _, present := GET(args.UserId + ":last"); present{
			reply.Status = tribproto.EEXISTS
	
	}else {
			//initialize the 3 maps for the user
			PUT(args.UserID + ":last", json.Marshal(0))
			PUT(args.UserID + ":subscribers", json.Marshal(make (map [string] bool))
			PUT(args.UserID + ":tribbles", json.Marshal(make (map [int] bool)) 
	}
	
	return nil
}

func (ts *Tribserver) AddSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) os.Error{
    
		//check if user exists
	  if _, present := GET(args.Userid + ":last"); present{
			
			//check if target user exists
			if _, present := GET(args.Targetuser + ":last"); present{
				addToList(args.Targetuser + ":subscribers", json.Marshal(args.Userid)) 					
			}else{
				reply.Status = tribproto.ENOSUCHTARGETUSER
			}

		}else{
			reply.Status = tribproto.ENOSUCHUSER
		}

		return nil
}

func (ts *Tribserver) RemoveSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) os.Error {
		//check if user exists
    if _, present := GET(args.Userid + ":last"); present{
			
			//check if target user exists
      if _, present := GET(args.Targetuser + ":last"); present{
					removeFromList(args.Targetuser + ":subscribers", json.Marshal(args.Userid))
			
			}else{
					reply.Status = tribproto.ENOSUCHTARGETUSER
			}
		}else{
				reply.Status = tribproto.ENOSUCHUSER
		}

		return nil
}

func (ts *Tribserver) GetSubscriptions(args *tribproto.GetSubscriptionsArgs, reply *tribproto.GetSubscriptionsReply) os.Error {
	if suscribersJson, present := GET(args.Userid + ":suscribers"); present {
		suscribers := make(map [string] bool)
		json.Unmarshal(suscribersJson, suscribers)

		for key, _ := range suscribers{
			reply.Userids << key
	  }

	}else{
		reply.Status = tribproto.ENOSUCHUSER
	}

	return nil
}

func (ts *Tribserver) PostTribble(args *tribproto.PostTribbleArgs, reply *tribproto.PostTribbleReply) os.Error {
	if lastJson, present := GET(args.Userid + ":last"); present{ 
		 last := new(int)
		 json.Unmarshal(lastJson, last)
		 lastStr := fmt.Sprintf(":%d", last)

		 tribble := new(tribproto.Tribble)
		 tribble.Userid := args.Userid
		 tribble.Posted := time.Nanoseconds()
		 tribble.Contents := args.Contents

		 PUT(args.Userid + ":tribbles", json.Marshal(tribble.Userid))
		 PUT(arg.Userid + ":" + lastStr, json.Marshal(tribble)) 
		 PUT(arg.Userid + ":last", json.Marshal(last + 1))
 
  }else{
		reply.Status = tribproto.ENOSUCHUSER
	}

	return nil
}

func (ts *Tribserver) GetTribbles(args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) os.Error {
	if tribidJson, present := GET(args.Userid + ":tribbles"); present{
		tribbles := json.Unmarshal(tribidJson)
	
		lastJson, _ := GET(args.Userid + ":last")	
		last := json.Unmarshal(lastJson)

		
		for i := last; i > 0 &&	len(reply.Tribbles) < 100; i--{
			if tribJson, present := GET(args.Userid + ":" + tribbles[i]); present {
				trib := new(Tribble)
				json.Unmarshal(tribJson, trib)
				reply.Tribbles << trib
			}				
		}
			
	}else{
		reply.Status = tribproto.ENOSUCHUSER
	}
		
	return nil
}

func (ts *Tribserver) GetTribblesBySubscription(args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) os.Error {
  if suscribJson, present := GET(args.Userid + ":suscribers"); present{
		suscribers := make(map [string] bool)
		json.Unmarshal(suscribJson, suscribers)
		recentTribs := new(MinHeap)

		for suscriber, _ := range suscribers {
	    
			lastJson, _ := GET(args.Userid + ":last")
		  last := json.Unmarshal(lastJson)
			lastStr := fmt.Sprintf(":%d", last)				
		
			if tribJson, present := GET(args.Userid + ":" + last); present {
				trib := json.Unmarshal(tribJson)				
				if recentTribs.Len() == 0 {
					recentTribs.Insert(trib)				
				}else if recentTribs.Len() > 100 && recentTribs[0].Posted() < trib.Posted() {
					recentTribs.Pop()
					recentTribs.Push(trib)			
				}else if recentTribs.Len() < 100{
					recentTribs.Push(trib)				
				}  			
			}							  
		}
	}
					
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
