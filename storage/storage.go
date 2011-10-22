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

func (tm *TribMap) AddToList(key string, element []byte) (os.Error, int) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	_, ok := tm.data[key]
	if (!ok) {
		return nil, storageproto.EKEYNOTFOUND
	}
	set, err := tm.GetSet(key)
	if (err != nil) {
		return err, storageproto.EPUTFAILED
	}
	s := new(string)
	err = json.Unmarshal(element, s)
	if (err != nil) {
		return err, storageproto.EPUTFAILED
	}
	_, ok = set[*s]
	if (ok) {
		return nil, storageproto.EITEMEXISTS
	}
	set[*s] = true
	val, err := json.Marshal(set)
	if (err != nil) {
		return err, storageproto.EPUTFAILED
	}
	tm.data[key] = val
	return nil, storageproto.OK
}

func (tm *TribMap) RemoveFromList(key string, element []byte) (os.Error, int) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	_, ok := tm.data[key]
	if (!ok) {
                return nil, storageproto.EKEYNOTFOUND
        }
	set, err := tm.GetSet(key)
	if (err != nil) {
		return err, storageproto.EPUTFAILED
	}
	s := new(string)
        err = json.Unmarshal(element, s)
        if (err != nil) {
                return err, storageproto.EPUTFAILED
        }
	_, ok = set[*s]
	if (!ok) {
		return nil, storageproto.EITEMNOTFOUND
	}
        set[*s] = false, false
	val, err := json.Marshal(set)
	if (err != nil) {
		return err, storageproto.EPUTFAILED
	}
	tm.data[key] = val
	return nil, storageproto.OK
}

func (tm *TribMap) GetSet(key string) (map[string] bool, os.Error) {
	set := make(map[string] bool)
	err := json.Unmarshal(tm.data[key], &set)
	if (err != nil) {
		return nil, err
	}
	return set, nil
}
