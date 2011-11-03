package storageserver

import (
	"storageproto"
	"hash/fnv"
	"strings"
	"rpc"
	"storage"
	"os"
	"time"
	"sync"
	"log"
	"net"
	"json"
	"container/vector"
	"container/list"
	"sort"
)

const (
	QUERY_COUNT_SECONDS = 5  // Request lease if this is the 3rd+
	QUERY_COUNT_THRESH = 3   // query in the last 5 seconds
	LEASE_SECONDS = 5       // Leases are valid for 5 seconds
	LEASE_GUARD_SECONDS = 1  // Servers add a short guard time
)

type Storageserver struct {
	tm *storage.TribMap
	leases *LeaseMap //stores the leases granted key -> leaselist
	cache *CacheMap
	servers []interface{} //stores the existing storage servers
	serversMap map[uint32] *serverData
	numnodes int
	nodeData storageproto.Client
	cond *sync.Cond
	master string
	
}

type CacheMap struct {
	cache *storage.TribMap
 	cacheInfo map[string] *cacheData
	lock sync.RWMutex
}

type LeaseMap struct {
	data map[string] *LeaseList
	lock sync.RWMutex
}

type LeaseList struct {
  list *list.List
  grantable bool 
}

type LeaseRequest struct {
	client storageproto.Client
	requestTime int64 
}

type cacheData struct {
	leaseTime int64
	requests *list.List
}

type serverData struct {
	rpc *rpc.Client     //connection to client
	clientInfo storageproto.Client  //info on client
}

func (c *serverData) Less(y interface{}) bool {
        return c.clientInfo.NodeID < y.(*serverData).clientInfo.NodeID
}

func newLeaseMap() *LeaseMap {
	lm := &LeaseMap{data:make(map[string] *LeaseList)}
	return lm
}

func newCacheMap() *CacheMap {
	cm := &CacheMap{cache:storage.NewTribMap(), cacheInfo:make(map[string] *cacheData)}
	return cm
}

func NewStorageserver(master string, numnodes int, portnum int, nodeid uint32) *Storageserver {
	ss := &Storageserver{storage.NewTribMap(), newLeaseMap(), newCacheMap(), 
		nil,  make(map[uint32] *serverData), numnodes, 
		storageproto.Client{"", nodeid}, sync.NewCond(&sync.Mutex{}), master}
	return ss
}

func (ss *Storageserver) Connect(addr net.Addr) {
	ss.nodeData.HostPort = addr.String()
	if (ss.numnodes == 0) {
		log.Printf("Connecting as slave")
		for {
			master, err := rpc.DialHTTP("tcp", ss.master)
			if err != nil {
				log.Printf("Failed to connect to the master; retrying: %s", err.String())
				time.Sleep(1000000000)
			} else {
				reply := new(storageproto.RegisterReply)
				err = master.Call("StorageRPC.Register",&storageproto.RegisterArgs{ss.nodeData}, reply)
				if (err != nil) {
					log.Printf("Something went wrong with the call; retrying")
				} else if reply.Ready {
					ss.servers = make([]interface{}, len(reply.Clients))
					for i:=0; i < len(reply.Clients); i++ {
						ss.servers[i] = &serverData{nil, reply.Clients[i]}
						ss.serversMap[reply.Clients[i].NodeID] = ss.servers[i].(*serverData)
					}
					break
				}
				time.Sleep(1000000000)
			}
		}
	} else {
		ss.servers = make([]interface{}, ss.numnodes)
		ss.servers[ss.numnodes - 1] = &serverData{nil, ss.nodeData}
		ss.serversMap[ss.nodeData.NodeID] = ss.servers[ss.numnodes - 1].(*serverData)
		ss.numnodes--
	}
	ss.cond.L.Lock()
	defer ss.cond.L.Unlock()
	log.Printf("Connecting to other servers")
	for ss.numnodes > 0 {
		ss.cond.Wait()
	}
	for i:=0; i < len(ss.servers); i++ {
		server := ss.servers[i].(*serverData)
		if server.clientInfo.NodeID == ss.nodeData.NodeID {
			continue
		}
		for {
			rpc, err := rpc.DialHTTP("tcp", server.clientInfo.HostPort)
			if (err != nil) {
				log.Printf("Problem making rpc connection")
			} else {
				server.rpc = rpc
				log.Printf("Made connection to %s", server.clientInfo.HostPort)
				break;
			}
			time.Sleep(1000000000)
		}
    }
	ss.sortServers()
   for i:=0; i < len(ss.servers); i++ {
	log.Printf("server %v", ss.servers[i])
   }
}

// You might define here the functions that the locally-linked application
// logic can use to call things like Get, GetList, Put, etc.
// OR, you can have them access the local storage node
// by calling the RPC functions.  Your choice!


// RPC-able interfaces, bridged via StorageRPC.
// These should do something! :-)

func (ss *Storageserver) RegisterRPC(args *storageproto.RegisterArgs, reply *storageproto.RegisterReply) os.Error {
	ss.cond.L.Lock()
	defer ss.cond.L.Unlock()
	reply.Ready = false
	if (ss.numnodes > 0) {
		log.Printf("Registration from %s", args.ClientInfo.HostPort)
		server := &serverData{}
		server.clientInfo = args.ClientInfo
		ss.servers[ss.numnodes - 1] = server
		ss.serversMap[server.clientInfo.NodeID] = server
		ss.numnodes--
	}
	if (ss.numnodes == 0) {
		reply.Ready = true
		reply.Clients = make([]storageproto.Client, len(ss.servers))
		for i:=0; i < len(ss.servers); i++ {
			reply.Clients[i] = ss.servers[i].(*serverData).clientInfo
		}
		ss.cond.Broadcast()
	}
	return nil
}

func (ss *Storageserver) handleLeaseRequest(args *storageproto.GetArgs) bool {
     ss.leases.lock.Lock()
	 defer ss.leases.lock.Unlock()

		if _, present := ss.leases.data[args.Key]; !present {	
				//make new list and insert lease request
				ss.leases.data[args.Key] = &LeaseList{list.New(), true}
				ss.leases.data[args.Key].list.PushBack(&LeaseRequest{args.LeaseClient, time.Nanoseconds()})	
				
				return true

		}else if ss.leases.data[args.Key].grantable == true{	
				ss.leases.data[args.Key].list.PushBack(&LeaseRequest{args.LeaseClient, time.Nanoseconds()})	
	  		
				return true
	 	}
		return false				  
}

func (ss *Storageserver) GetRPC(args *storageproto.GetArgs, reply *storageproto.GetReply) os.Error {
	reply.Lease = storageproto.LeaseStruct{}
	reply.Lease.Granted = false
	reply.Lease.ValidSeconds = 0
	if args.WantLease && ss.handleLeaseRequest(args) {
		reply.Lease.Granted = true
		reply.Lease.ValidSeconds = LEASE_SECONDS
    }

	val, status := ss.tm.GET(args.Key)
	reply.Status = status
	log.Printf("Getting from local storage based on rpc req")
	
	if (status == storageproto.OK) {
		reply.Value = string(val)
	}
	return nil
}

func (ss *Storageserver) GetListRPC(args *storageproto.GetArgs, reply *storageproto.GetListReply) os.Error {
	reply.Lease = storageproto.LeaseStruct{}
	 
	reply.Lease.Granted = false
	reply.Lease.ValidSeconds = 0
	if args.WantLease && ss.handleLeaseRequest(args) {
		reply.Lease.Granted = true
		reply.Lease.ValidSeconds = LEASE_SECONDS
    }

	val, status := ss.tm.GET(args.Key)
	reply.Status = status
	if (status == storageproto.OK) {
		set := make(map[string] bool)
		err := json.Unmarshal(val, &set)
		if (err != nil) {
			reply.Status = storageproto.EPUTFAILED
		} else {
			reply.Value = make([]string, len(set))
			i:=0
			for key, _ := range set {
				reply.Value[i]=key
				i++
			}
		}
	}
	return nil
}

func (ss *Storageserver) handleModRequest(key string){
  ss.leases.lock.Lock()
	if leaseList, present := ss.leases.data[key]; !(present && leaseList.list.Len() > 0) {
		ss.leases.lock.Unlock()
	 	return
	}
	leaseRequest := ss.leases.data[key].list
	ss.leases.data[key].grantable = false; //deny all other lease requests
  ss.leases.data[key].list = list.New()
  ss.leases.lock.Unlock()

	//call RevokeLeaseReply to all lease holders
	for leaseElement := leaseRequest.Front(); leaseElement != nil; leaseElement = leaseElement.Next() {
		lease := leaseElement.Value.(*LeaseRequest)
		//wait for OK reply or until lease expires
		for (time.Nanoseconds() - lease.requestTime) < (LEASE_SECONDS + LEASE_GUARD_SECONDS) * 1000000000 {
			reply := new(storageproto.RevokeLeaseReply)
			rpcClient := ss.serversMap[lease.client.NodeID]	
			err := rpcClient.rpc.Call("StorageRPC.RevokeLease",&storageproto.RevokeLeaseArgs{key}, reply)	
		   		
			if (err != nil) {
				log.Printf("Something went wrong with revoke rpc call")		
			}else if reply.Status == storageproto.OK {
				log.Printf("client %v replied ok for Revoke", lease.client.NodeID)
				break;
			}
		}
	}

  ss.leases.lock.Lock()
	ss.leases.data[key].grantable = true 
  ss.leases.lock.Unlock()
}


func (ss *Storageserver) PutRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	ss.handleModRequest(args.Key)	
	
	status := ss.tm.PUT(args.Key, []byte(args.Value))
	reply.Status = status
	return nil
}

func (ss *Storageserver) AppendToListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	ss.handleModRequest(args.Key) 
	status, err := ss.tm.AddToList(args.Key, []byte(args.Value))
	reply.Status = status
	return err
}

func (ss *Storageserver) RemoveFromListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	ss.handleModRequest(args.Key) 
	 
	status, err := ss.tm.RemoveFromList(args.Key, []byte(args.Value))
	reply.Status = status
	return err
}

func (ss *Storageserver) RevokeLeaseRPC(args *storageproto.RevokeLeaseArgs, reply *storageproto.RevokeLeaseReply) os.Error {
	ss.cache.lock.Lock()
	defer ss.cache.lock.Unlock()
	ss.cache.cacheInfo[args.Key] = nil, false
	
	reply.Status = storageproto.OK
	log.Printf("Revoking key %s", args.Key)
	return nil
}

func (ss *Storageserver) GET(key string) ([]byte, int, os.Error) {
  	index := ss.GetIndex(key)
	serv := ss.servers[index].(*serverData)
	//if local server call server tribmap
	if serv.clientInfo.NodeID == ss.nodeData.NodeID {
		log.Printf("Getting from local storage")
		res, stat := ss.tm.GET(key)
		return res, stat, nil
	}
	ss.cache.lock.Lock()
	defer ss.cache.lock.Unlock()
	
	data, ok := ss.cache.cacheInfo[key]
	if !ok {
		data = &cacheData{0, list.New()}
		ss.cache.cacheInfo[key] = data
	}
	data.requests.PushBack(time.Nanoseconds())
	if data.requests.Len() > QUERY_COUNT_THRESH {
		data.requests.Remove(data.requests.Front())
	}
	if (data.leaseTime - time.Nanoseconds() > 0) {
		log.Printf("Getting from cache %s", key);
		res, stat := ss.cache.cache.GET(key)
		return res, stat, nil
	}
	log.Printf("Request len %d for %s", data.requests.Len(), key)
	var requestLease bool = false
	if (data.requests.Len() >= QUERY_COUNT_THRESH && data.requests.Back().Value.(int64) - data.requests.Front().Value.(int64) <= QUERY_COUNT_SECONDS * 1000000000) {
		requestLease = true
		log.Printf("Requesting lease for %s", key)
	}
	
	log.Printf("Getting from %s", serv.clientInfo.HostPort)
	//if not, call correct server via rpc
	args := &storageproto.GetArgs{key, requestLease, ss.nodeData}
	reply := new(storageproto.GetReply)
	err := serv.rpc.Call("StorageRPC.Get", args, reply)
	if (err != nil) {
		return nil, 0, err
	}
	if (reply.Lease.Granted) {
		data.leaseTime = time.Nanoseconds() + int64(reply.Lease.ValidSeconds * 1000000000)
		ss.cache.cache.PUT(key, []byte(reply.Value))
		log.Printf("Adding to cache %s", key)
	}
	
	return []byte(reply.Value), reply.Status, nil
}

func (ss *Storageserver) PUT(key string, val []byte) (int, os.Error) {
	index := ss.GetIndex(key)
	serv := ss.servers[index].(*serverData)

	//if local server call server tribmap
	if serv.clientInfo.NodeID == ss.nodeData.NodeID {
		ss.handleModRequest(key)
		return ss.tm.PUT(key, val), nil
	}
	//if not, call correct server via rpc
	args := &storageproto.PutArgs{key, string(val)}
	var reply storageproto.PutReply
	err := serv.rpc.Call("StorageRPC.Put", args, &reply)
	if (err != nil) {
		return 0, err
	}
	return reply.Status, nil
}

func (ss *Storageserver) AddToList(key string, element []byte) (int, os.Error) {
	index := ss.GetIndex(key)
	serv := ss.servers[index].(*serverData)

	//if local server call server tribmap
	if serv.clientInfo.NodeID == ss.nodeData.NodeID {
		ss.handleModRequest(key)
		return ss.tm.AddToList(key, element)
	}
	//if not, call correct server via rpc
	args := &storageproto.PutArgs{key, string(element)}
	var reply storageproto.PutReply
	err := serv.rpc.Call("StorageRPC.AppendToList", args, &reply)
	if (err != nil) {
		return 0, err
	}
	return reply.Status, nil
}

func (ss *Storageserver) RemoveFromList(key string, element []byte) (int, os.Error) {
	//get server with user info
	index := ss.GetIndex(key)
	serv := ss.servers[index].(*serverData)

	//if local server call server tribmap
	if serv.clientInfo.NodeID == ss.nodeData.NodeID {
		ss.handleModRequest(key)
		return ss.tm.RemoveFromList(key, element)
	}
	//if not, call correct server via rpc
	args := &storageproto.PutArgs{key, string(element)}
	var reply storageproto.PutReply
	err := serv.rpc.Call("StorageRPC.RemoveFromList", args, &reply)
	if (err != nil) {
		return 0, err
	}
	return reply.Status, nil
}

/** function to get index in server array for the correct server 
     based on the key given **/
func (ss *Storageserver) GetIndex(key string) (int) {
	//get userID
	fields := strings.Split(key, ":")
	user := fields[0]
	h := fnv.New32()
	h.Write([]byte(user))
	hash := h.Sum32()

	//get nodeid
	pos := sort.Search(len(ss.servers), func(i int) bool { 
		return ss.servers[i].(*serverData).clientInfo.NodeID >= hash 
	})

	//mod with server array size
	return pos  % len(ss.servers)
}

func (ss *Storageserver) sortServers() {
	v := vector.Vector(ss.servers)
	sort.Sort(&v)
}

