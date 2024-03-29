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
	"rand"
	"container/vector"
	"container/heap"
	"storageproto"
	"storageserver"
	"storagerpc"
)
//list of a user's subscriptions is stored as <user>:subscriptions
const SUBSCRIPTIONS string = ":subscriptions"

//counter of tribbles for a user is stored as <user>:last
const LAST string = ":last"

func (ts *Tribserver) CreateUser(args *tribproto.CreateUserArgs, reply *tribproto.CreateUserReply) os.Error {
	//check if user exists
	 _, stat, err := ts.tm.GET(args.Userid + LAST)
	if (err != nil) {
		return err
	}

	//return if exists already
	if stat == storageproto.OK {
		log.Printf("create user: user already exists")
		reply.Status = tribproto.EEXISTS

	} else  {
		log.Printf("Creating new user")
		//initialize the 3 maps for the user
		val, _ := json.Marshal(0)
		ts.tm.PUT(args.Userid + LAST, val)
		val, _ = json.Marshal(make(map[string] bool))
		ts.tm.PUT(args.Userid + SUBSCRIPTIONS, val)
		log.Printf("FInished creating new user %s", args.Userid)
	}

	log.Printf("returning nil from create user")
	return nil
}



func (ts *Tribserver) AddSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) os.Error{
	//check if user exists
	_, stat, err := ts.tm.GET(args.Userid + LAST)
	if (err != nil) {
		return err
	}
	if stat == storageproto.OK{
		//check if target user exists
		_, stat, err := ts.tm.GET(args.Targetuser + LAST);
		if err != nil {
			return err
		}
		if stat == storageproto.OK {
			val, err := json.Marshal(args.Targetuser)
			if (err != nil) {
				return err
			}
			//add suscription to user's list
			status, err := ts.tm.AddToList(args.Userid + SUBSCRIPTIONS, val)
			if (err != nil){
				return err
			}else if status == storageproto.EITEMEXISTS{
				reply.Status = tribproto.EEXISTS
			}

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
	if stat == storageproto.OK {
		//check if target user exists
		_, stat, err := ts.tm.GET(args.Targetuser + LAST)
		if (err != nil) {
			return err
		}
		if stat == storageproto.OK{
			val, err := json.Marshal(args.Targetuser)
			if (err != nil) {
				return err
			}

			//remove subscription from user
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
	//check if user exists
	suscribersJson, stat, err := ts.tm.GET(args.Userid + SUBSCRIPTIONS)
	if (err != nil) {
		return err
	}
	if stat == storageproto.OK {
		subscribers := make(map [string] bool)
		err := json.Unmarshal(suscribersJson, &subscribers)
		if (err != nil) {
			return err
		}
		reply.Userids = make([]string, len(subscribers))
		i:=0

		//add each userid in the suscribers list to the reply arg
		for key, _ := range subscribers{
			s := new(string)
			err = json.Unmarshal([]byte(key), s)
			if (err != nil) {
				return err
			}
			reply.Userids[i] = *s
			i++
		}
		reply.Status = tribproto.OK
	}else{
		reply.Status = tribproto.ENOSUCHUSER
	}
	return nil
}

func (ts *Tribserver) PostTribble(args *tribproto.PostTribbleArgs, reply *tribproto.PostTribbleReply) os.Error {
	//check that the user exists
	lastJson, stat, err := ts.tm.GET(args.Userid + LAST)
	if (err != nil) {
		return err
	}
	if stat == storageproto.OK {
		 last := new(int)
		 json.Unmarshal(lastJson, last)
		 *last++
		 lastStr := fmt.Sprintf(":%d", *last)

     //make new tribble struct with args
		 tribble := new(tribproto.Tribble)
		 tribble.Userid = args.Userid
		 tribble.Posted = time.Nanoseconds()
		 tribble.Contents = args.Contents

		 val, err := json.Marshal(tribble)
		if (err != nil) {
			return err
		}

		//put in the storage system
		_, err = ts.tm.PUT(args.Userid + lastStr, val)
		if (err != nil) {
			return err
		}
		val, err = json.Marshal(*last)
		if (err != nil) {
			return err
		}

		//update tribble counter
		ts.tm.PUT(args.Userid + LAST, val)
	} else {
		reply.Status = tribproto.ENOSUCHUSER
	}
	return nil
}

func (ts *Tribserver) GetTribbles(args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) os.Error {
	//check if user exists
	lastJson, stat, err := ts.tm.GET(args.Userid + LAST)
	if err != nil {
		return err
	}
	if stat == storageproto.OK{
		last := new(int)
		err := json.Unmarshal(lastJson, last)
		if (err != nil) {
			return err
		}
		size := *last
		if (size > 100) {
			size = 100
		}

		//get 100 latest tribbles from user and add to the reply arg
		reply.Tribbles = make([]tribproto.Tribble, size)
		for i := 0; i < size; i++ {
			tribbleId := fmt.Sprintf("%s:%d", args.Userid, *last)
			tribJson, stat, err := ts.tm.GET(tribbleId)
			if err != nil{
				return err
			}
			if stat == storageproto.OK {
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
	//check if user exists
	suscribJson, stat, err := ts.tm.GET(args.Userid + SUBSCRIPTIONS)
	if err != nil {
		return err
	}
	if stat == storageproto.OK{
		suscribers := make(map [string] bool)
		err = json.Unmarshal(suscribJson, &suscribers)
		if (err != nil) {
			return err
		}

		//make minheap to store recent tribbles
		recentTribs := vector.Vector(make([]interface{},0, 100))
		heap.Init(&recentTribs)
		level := 0
		hasTribs := false
		
		//continue adding tribbles to heap until we reach a 100 or no more tribbles
		for recentTribs.Len() < 100 {
			hasTribs = false
			
			//for each suscription, get the last tribble
			for subscriber, _ := range suscribers  {
				s := new(string)
				err = json.Unmarshal([]byte(subscriber), s)
				if err != nil {
					return err
				}

				lastJson, _, err := ts.tm.GET(*s + LAST)
				if err != nil{
					return err
				}
				last := new(int)
				err = json.Unmarshal(lastJson, last)
				if (err != nil) {
					return err
				}
				*last -=level
				tribbleId := fmt.Sprintf("%s:%d", *s, *last)
				if (*last > 0) {
					hasTribs = true
					tribJson, stat, err := ts.tm.GET(tribbleId)
					if err != nil {
						return err
					}
					if stat == storageproto.OK {
						trib := new(tribproto.Tribble)
						err = json.Unmarshal(tribJson, trib)
						if (err != nil) {
							return err
						}

						//add to tribble if more recent than oldest tribble in heap
						if recentTribs.Len() >= 100 && recentTribs[0].(tribproto.Tribble).Posted < trib.Posted {
							heap.Pop(&recentTribs)
							heap.Push(&recentTribs, trib)

						//add to tribble if less than 100 in heap
						}else if recentTribs.Len() < 100{
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
	if *nodeID ==0 {
		rand.Seed(time.Nanoseconds())
		*nodeID = uint(rand.Uint32())
	}
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
	log.Printf("Establishing connection with storage servers")
	go ss.Connect(l.Addr())
	http.Serve(l, nil)
}
