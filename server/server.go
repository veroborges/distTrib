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
	tm *storage.TribMap // Reference to the storage system
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
			//initialize the 2 maps for the user
			val, _ := json.Marshal(0)
			ts.tm.PUT(args.Userid + LAST, val) // the count to zero
			val, _ = json.Marshal(make(map[string] bool))
			ts.tm.PUT(args.Userid + SUBSCRIPTIONS, val) // set the subscriptions to an empty set
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
			// add the target user to the subscriptions
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
			// Remove the target user from the subscriptions
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
		// Read all subscriptions of the user
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
		 *last++ // get last trib id and increment it
		 lastStr := fmt.Sprintf(":%d", *last)

		 tribble := new(tribproto.Tribble)
		 tribble.Userid = args.Userid
		 tribble.Posted = time.Nanoseconds()
		 tribble.Contents = args.Contents

		 val, err := json.Marshal(tribble)
		if (err != nil) {
			return err
		}
		ts.tm.PUT(args.Userid + lastStr, val) // store the tribble with new trib id
		val, err = json.Marshal(*last)
		if (err != nil) {
			return err
		}
		ts.tm.PUT(args.Userid + LAST, val) // increment the last trib id
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
		if (size > 100) { // read at most 100 messages
			size = 100
		}
		reply.Tribbles = make([]tribproto.Tribble, size)
		for i := 0; i < size; i++ {
			tribbleId := fmt.Sprintf("%s:%d", args.Userid, *last) // get latest message
			if tribJson, present := ts.tm.GET(tribbleId); present {
				trib := new(tribproto.Tribble)
				err := json.Unmarshal(tribJson, trib)
				if (err != nil) {
					return err
				}
				reply.Tribbles[i] = *trib
			}
			*last-- // decremeent last
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
		recentTribs := vector.Vector(make([]interface{},0, 100)) // create a vector with at most 100 elements
		heap.Init(&recentTribs) //convert it to a min heap
		level := 0
		hasTribs := true
		for recentTribs.Len() < 100 && hasTribs { // loop until we get 100 tribs or there are no more subcribers with tribs left
			hasTribs = false
			for subscriber, _ := range suscribers  {
				lastJson, _ := ts.tm.GET(subscriber + LAST)
				last := new(int)
				err := json.Unmarshal(lastJson, last)
				if (err != nil) {
					return err
				}
				*last -=level // get last n-th message for each subscriber
				tribbleId := fmt.Sprintf("%s:%d", subscriber, *last)
				if (*last > 0) {
					hasTribs = true // we found a subscriber that has at least one message
					if tribJson, present := ts.tm.GET(tribbleId); present {
						trib := new(tribproto.Tribble)
						err = json.Unmarshal(tribJson, trib)
						if (err != nil) {
							return err
						}
						// if the heap has more than 100 elements, include only if the tribble is posted after the root
						// to ensure that the heap only contains 100 latest messages
						if recentTribs.Len() >= 100 && recentTribs[0].(tribproto.Tribble).Posted < trib.Posted {
							heap.Pop(&recentTribs) // throway the trib at the root
							heap.Push(&recentTribs, trib)
						}else if recentTribs.Len() < 100{ // we have less than 100 elements, just stick it into the heap
							heap.Push(&recentTribs, trib)
						}
					}
				}
			}
			level++ // increment the level in order to look at the next message from the end
		}
		reply.Tribbles = make([]tribproto.Tribble, recentTribs.Len())
		for i:=recentTribs.Len() - 1; i >=0; i-- {
			reply.Tribbles[i] = *(heap.Pop(&recentTribs).(*tribproto.Tribble)) // return tribbles in reverse chronological order
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
