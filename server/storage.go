package storage

import (
	"sync"
	"os"
	"json"
)

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
	return true
}

func (tm *TribMap) AddToList(key string, element []byte) (os.Error) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	set, err := tm.getSet(key)
	if (err != nil) {
		return err
	}
	set[string(element)] = true
	val, err := json.Marshal(set)
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
	set[string(element)] = false, false
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
	set := make(map[string] bool);
	err := json.Unmarshal(val, set)
	if (err != nil) {
		return nil, err
	}
	return set, nil
}
