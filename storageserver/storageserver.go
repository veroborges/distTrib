package storageserver

import (
	"storageproto"
)

type Storageserver struct {
}

func NewStorageserver(master string, numnodes int, portnum int, nodeid uint32) *Storageserver {
	if (numnodes > 0) {
		// I'm the master!  That's exciting!
	}
	ss := &Storageserver{}
	return ss
}

// You might define here the functions that the locally-linked application
// logic can use to call things like Get, GetList, Put, etc.
// OR, you can have them access the local storage node
// by calling the RPC functions.  Your choice!


// RPC-able interfaces, bridged via StorageRPC.
// These should do something! :-)

func (ss *Storageserver) RegisterRPC(args *storageproto.RegisterArgs, reply *storageproto.RegisterReply) os.Error {
	return nil
}

func (ss *Storageserver) GetRPC(args *storageproto.GetArgs, reply *storageproto.GetReply) os.Error {
	return nil
}

func (ss *Storageserver) GetListRPC(args *storageproto.GetArgs, reply *storageproto.GetListReply) os.Error {
	return nil
}

func (ss *Storageserver) PutRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	return nil
}

func (ss *Storageserver) AppendToListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	return nil
}

func (ss *Storageserver) RemoveFromListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) os.Error {
	return nil
}




type TribMap struct {
	data map[string] []byte
	lock sync.RWMutex
}

func NewTribMap() *TribMap {
	tm := &TribMap{data:make(map[string] []byte)}
	return tm
}

func (tm *TribMap) GET(key string) ([]byte, bool) {
	tm.lock.RLock()
	defer tm.lock.RUnlock()
	val, ok := tm.data[key]
	return val, ok
}

func (tm *TribMap) PUT(key string, val []byte) (bool) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	tm.data[key] = val
	log.Printf("Adding %s to %s", string(val), key)
	return true
}

func (tm *TribMap) AddToList(key string, element []byte) (os.Error) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	set, err := tm.getSet(key)
	if (err != nil) {
		return err
	}
	s := new(string)
	err = json.Unmarshal(element, s)
	if (err != nil) {
		return err
	}
	set[*s] = true
	val, err := json.Marshal(set)
	log.Printf("%s %s", key, string(tm.data[key]))
	if (err != nil) {
		return err
	}
	tm.data[key] = val
	return nil
}

func (tm *TribMap) RemoveFromList(key string, element []byte) (os.Error) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	set, err := tm.getSet(key)
	if (err != nil) {
		return err
	}
	s := new(string)
        err = json.Unmarshal(element, s)
        if (err != nil) {
                return err
        }
        set[*s] = false, false
	val, err := json.Marshal(set)
	if (err != nil) {
		return err
	}
	tm.data[key] = val
	return nil
}

func (tm *TribMap) getSet(key string) (map[string] bool, os.Error) {
	val, ok := tm.data[key]
	if (!ok) {
		return nil, os.NewError("Non-existing key")
	}
	set := make(map[string] bool)
	err := json.Unmarshal(val, &set)
	if (err != nil) {
		return nil, err
	}
	return set, nil
}
