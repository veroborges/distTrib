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
	list, err := tm.getMap(key)
	if (err != nil) {
		return err
	}
	list[string(element)] = true
	return nil
}

func (tm *TribMap) RemoveFromList(key string, element []byte) (os.Error) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	list, err := tm.getMap(key)
	if (err != nil) {
		return err
	}
	list[string(element)] = false, false
	return nil
}

func (tm *TribMap) getMap(key string) (map[string] bool, os.Error) {
	val, ok := tm.data[key]
	if (!ok) {
		return nil, os.NewError("Non-existing key")
	}
	list := make(map[string] bool);
	err := json.Unmarshal(val, list)
	if (err != nil) {
		return nil, err
	}
	return list, nil
}