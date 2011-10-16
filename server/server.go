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
	"storage"
	"container/vector"
	"container/heap"
)

const SUBSCRIPTIONS string = ":subscriptions"
const LAST string = ":last"

//tribServer Struct


type Tribserver struct {
	tm *storage.TribMap
}

func NewTribserver() *Tribserver {
	ts := &Tribserver{storage.NewTribMap()}
	return ts
}

func (ts *Tribserver) CreateUser(args *tribproto.CreateUserArgs, reply *tribproto.CreateUserReply) os.Error {
	//if a map fot that user exists (present is true) return EEXISTS
	if _, present := ts.tm.GET(args.Userid + LAST); present{
			reply.Status = tribproto.EEXISTS
	} else {
			//initialize the 3 maps for the user
			val, _ := json.Marshal(0)
			ts.tm.PUT(args.Userid + LAST, val)
			val, _ = json.Marshal(make(map[string] bool))
			ts.tm.PUT(args.Userid + SUBSCRIPTIONS, val)
			//ts.tm.PUT(args.UserID + ":tribbles", json.Marshal(make (map [int] bool)) 
	}
	return nil
}



func (ts *Tribserver) AddSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) os.Error{
	//check if user exists
	if _, present := ts.tm.GET(args.Userid + LAST); present{
		//check if target user exists
		if _, present := ts.tm.GET(args.Targetuser + LAST); present {
			val, err := json.Marshal(args.Targetuser)
			if (err != nil) {
				return err
			}
			ts.tm.AddToList(args.Userid + SUBSCRIPTIONS, val)
		} else {
			reply.Status = tribproto.ENOSUCHTARGETUSER
		}
	} else {
		reply.Status = tribproto.ENOSUCHUSER
	}
	return nil
}

func (ts *Tribserver) RemoveSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) os.Error {
	//check if user exists
	if _, present := ts.tm.GET(args.Userid + LAST); present {
		//check if target user exists
		if _, present := ts.tm.GET(args.Targetuser + LAST); present{
			val, err := json.Marshal(args.Targetuser)
			if (err != nil) {
				return err
			}
			ts.tm.RemoveFromList(args.Userid + SUBSCRIPTIONS, val)
		} else {
			reply.Status = tribproto.ENOSUCHTARGETUSER
		}
	} else {
		reply.Status = tribproto.ENOSUCHUSER
	}
	return nil
}

func (ts *Tribserver) GetSubscriptions(args *tribproto.GetSubscriptionsArgs, reply *tribproto.GetSubscriptionsReply) os.Error {
	if suscribersJson, present := ts.tm.GET(args.Userid + SUBSCRIPTIONS); present {
		subscribers := make(map [string] bool)
		err := json.Unmarshal(suscribersJson, &subscribers)
		if (err != nil) {
			return err
		}
		reply.Userids = make([]string, len(subscribers))
		i:=0
		for key, _ := range subscribers{
			reply.Userids[i] = key
			i++
		}
		reply.Status = tribproto.OK
	}else{
		reply.Status = tribproto.ENOSUCHUSER
	}
	return nil
}

func (ts *Tribserver) PostTribble(args *tribproto.PostTribbleArgs, reply *tribproto.PostTribbleReply) os.Error {
	if lastJson, present := ts.tm.GET(args.Userid + LAST); present{ 
		 last := new(int)
		 json.Unmarshal(lastJson, last)
		 *last++
		 lastStr := fmt.Sprintf(":%d", *last)

		 tribble := new(tribproto.Tribble)
		 tribble.Userid = args.Userid
		 tribble.Posted = time.Nanoseconds()
		 tribble.Contents = args.Contents

		 //tm.PUT(args.Userid + ":tribbles", json.Marshal(tribble.Userid))
		 val, err := json.Marshal(tribble)
		if (err != nil) {
			return err
		}
		ts.tm.PUT(args.Userid + lastStr, val)
		val, err = json.Marshal(*last)
		if (err != nil) {
			return err
		}
		ts.tm.PUT(args.Userid + LAST, val)
	} else {
		reply.Status = tribproto.ENOSUCHUSER
	}
	return nil
}

func (ts *Tribserver) GetTribbles(args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) os.Error {
	if lastJson, present := ts.tm.GET(args.Userid + LAST); present{
		last := new(int)
		err := json.Unmarshal(lastJson, last)
		if (err != nil) {
			return err
		}
		size := *last
		if (size > 100) {
			size = 100
		}
		reply.Tribbles = make([]tribproto.Tribble, size)
		for i := 0; i < size; i++ {
			tribbleId := fmt.Sprintf("%s:%d", args.Userid, *last)
			if tribJson, present := ts.tm.GET(tribbleId); present {
				trib := new(tribproto.Tribble)
				err := json.Unmarshal(tribJson, trib)
				if (err != nil) {
					return err
				}
				reply.Tribbles[i] = *trib
			}
			*last--
		}
	}else{
		reply.Status = tribproto.ENOSUCHUSER
	}
	return nil
}

func (ts *Tribserver) GetTribblesBySubscription(args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) os.Error {
	if suscribJson, present := ts.tm.GET(args.Userid + SUBSCRIPTIONS); present{
		suscribers := make(map [string] bool)
		json.Unmarshal(suscribJson, &suscribers)
		recentTribs := vector.Vector(make([]interface{},0, 100))
		heap.Init(&recentTribs)
		level := 0
		hasTribs := false
		log.Printf("sub len %d", len(suscribers))
		for recentTribs.Len() < 100 {
			hasTribs = false
			for subscriber, _ := range suscribers  {
				log.Printf("get r %s", subscriber + LAST)
				lastJson, _ := ts.tm.GET(subscriber + LAST)
				log.Printf("last %s", string(lastJson))
				last := new(int)
				err := json.Unmarshal(lastJson, last)
				if (err != nil) {
					return err
				}
				*last -=level
				tribbleId := fmt.Sprintf("%s:%d", subscriber, *last)
				log.Printf("%s", tribbleId)
				if (*last > 0) {
					hasTribs = true
					if tribJson, present := ts.tm.GET(tribbleId); present {
						log.Printf("%s %s", tribbleId, tribJson)
						trib := new(tribproto.Tribble)
						err = json.Unmarshal(tribJson, trib)
						if (err != nil) {
							return err
						}
						if recentTribs.Len() >= 100 && recentTribs[0].(tribproto.Tribble).Posted < trib.Posted {
							log.Printf("poping and pushing")
							heap.Pop(&recentTribs)
							heap.Push(&recentTribs, trib)
						}else if recentTribs.Len() < 100{
							log.Printf("pushing")
							heap.Push(&recentTribs, trib)
						}
					}
				}
			}
			level++
			if (!hasTribs) {
				break
			}
		}
		reply.Tribbles = make([]tribproto.Tribble, recentTribs.Len())
		log.Printf("Len %d", recentTribs.Len())
		for i:=recentTribs.Len() - 1; i >=0; i-- {
			reply.Tribbles[i] = *(heap.Pop(&recentTribs).(*tribproto.Tribble))
		}
	} else {
		reply.Status = tribproto.ENOSUCHUSER
	}
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
