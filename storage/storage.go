package storage

import (
	"sync"
	"os"
	"json"
	"storageproto"
)

type TribMap struct {
	data map[string] []byte
	lock sync.RWMutex
}

func NewTribMap() *TribMap {
	tm := &TribMap{data:make(map[string] []byte)}
	return tm
}

func (tm *TribMap) GET(key string) ([]byte, int) {
	tm.lock.RLock()
	defer tm.lock.RUnlock()
	val, ok := tm.data[key]
	if (!ok) {
		return nil, storageproto.EKEYNOTFOUND
	}
	return val, storageproto.OK
}

func (tm *TribMap) PUT(key string, val []byte) (int) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	tm.data[key] = val
	return  storageproto.OK
}

func (tm *TribMap) AddToList(key string, element []byte) (int, os.Error) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	_, ok := tm.data[key]
	var set map[string] bool
	var err os.Error
	if (!ok) {
		set = make(map[string] bool)
	} else {
		set, err = tm.GetSet(key)
		if (err != nil) {
			return storageproto.EPUTFAILED, err
		}
	}
	s := string(element)
	_, ok = set[s]
	if (ok) {
		return storageproto.EITEMEXISTS, err
	}
	set[s] = true
	val, err := json.Marshal(set)
	if (err != nil) {
		return storageproto.EPUTFAILED, err
	}
	tm.data[key] = val
	return storageproto.OK, nil
}

func (tm *TribMap) RemoveFromList(key string, element []byte) (int, os.Error) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	_, ok := tm.data[key]
	if (!ok) {
                return storageproto.EKEYNOTFOUND, nil
        }
	set, err := tm.GetSet(key)
	if (err != nil) {
		return storageproto.EPUTFAILED, err
	}
	s := string(element)
	_, ok = set[s]
	if (!ok) {
		return storageproto.EITEMNOTFOUND, err
	}
        set[s] = false, false
	val, err := json.Marshal(set)
	if (err != nil) {
		return storageproto.EPUTFAILED, err
	}
	tm.data[key] = val
	return storageproto.OK, nil
}

func (tm *TribMap) GetSet(key string) (map[string] bool, os.Error) {
	set := make(map[string] bool)
	err := json.Unmarshal(tm.data[key], &set)
	if (err != nil) {
		return nil, err
	}
	return set, nil
}
