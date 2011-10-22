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
	"container/vector"
	"container/heap"
	"storageproto"
	"storageserver"
)

const SUBSCRIPTIONS string = ":subscriptions"
const LAST string = ":last"

func (ts *Tribserver) CreateUser(args *tribproto.CreateUserArgs, reply *tribproto.CreateUserReply) os.Error {
	//if a map fot that user exists (present is true) return EEXISTS
	 _, stat, err := ts.tm.GET(args.Userid + LAST)
	if (err != nil) {
		return err
	}
	if stat == storageproto.EITEMEXISTS {
			reply.Status = tribproto.EEXISTS
	} else  {
			//initialize the 3 maps for the user
			val, _ := json.Marshal(0)
			ts.tm.PUT(args.Userid + LAST, val)
			val, _ = json.Marshal(make(map[string] bool))
			ts.tm.PUT(args.Userid + SUBSCRIPTIONS, val)
	}
	return nil
}



func (ts *Tribserver) AddSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) os.Error{
	//check if user exists
	_, stat, err := ts.tm.GET(args.Userid + LAST)
	if (err != nil) {
		return err
	}
	if stat == storageproto.EITEMEXISTS{
		//check if target user exists
		_, stat, err := ts.tm.GET(args.Targetuser + LAST);
		if err != nil {
			return err
		}
		if stat == storageproto.EITEMEXISTS {
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
	_, stat,err := ts.tm.GET(args.Userid + LAST); 
	if (err != nil) {
		return err
	}
	if stat == storageproto.EITEMEXISTS {
		//check if target user exists
		_, stat, err := ts.tm.GET(args.Targetuser + LAST)
		if (err != nil) {
			return err
		}
		if stat == storageproto.EITEMEXISTS{
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
	suscribersJson, stat, err := ts.tm.GET(args.Userid + SUBSCRIPTIONS)
	if (err != nil) {
		return err
	}
	if stat == storageproto.EITEMEXISTS {
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
	lastJson, stat, err := ts.tm.GET(args.Userid + LAST)
	if (err != nil) {
		return err
	}
	if stat == storageproto.EITEMEXISTS {
		 last := new(int)
		 json.Unmarshal(lastJson, last)
		 *last++
		 lastStr := fmt.Sprintf(":%d", *last)

		 tribble := new(tribproto.Tribble)
		 tribble.Userid = args.Userid
		 tribble.Posted = time.Nanoseconds()
		 tribble.Contents = args.Contents

		 val, err := json.Marshal(tribble)
		if (err != nil) {
			return err
		}
		_ := ts.tm.PUT(args.Userid + lastStr, val)
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
	if lastJson, stat := ts.tm.GET(args.Userid + LAST); stat == storageproto.EITEMEXISTS{
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
			if tribJson, stat := ts.tm.GET(tribbleId); stat == storageproto.EITEMEXISTS {
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
	if suscribJson, stat := ts.tm.GET(args.Userid + SUBSCRIPTIONS); stat == storageproto.EITEMEXISTS{
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
					if tribJson, stat := ts.tm.GET(tribbleId); stat == storageproto.EITEMEXISTS {
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


type Tribserver struct {
	tm *storageserver.Storageserver
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
