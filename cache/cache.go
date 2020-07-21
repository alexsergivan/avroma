package cache

import (
	"fmt"
	"sync"
)

type Data struct {
	mutex  *sync.RWMutex
	data   map[uint32]interface{}
	counts map[uint32]uint32
}

func NewData() Data {
	m := &sync.RWMutex{}
	return Data{
		mutex:  m,
		data:   make(map[uint32]interface{}),
		counts: make(map[uint32]uint32),
	}
}

func (d Data) Get(key uint32) interface{} {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return d.data[key]
}

// Add adds value to the immutable magic map.
func (d Data) Add(key uint32, value interface{}) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.data[key] = value
	d.counts[key] = d.counts[key] + 1
}

// Del decrements the count and deletes if cont is 0.
func (d Data) Del(key uint32) {
	d.mutex.Lock()
	count := d.counts[key]
	count -= 1
	if count < 1 {
		delete(d.data, key)
		delete(d.counts, key)
	} else {
		d.counts[key] = count
	}
	d.mutex.Unlock()
}

// Kill removes it completely.
func (d Data) Kill(key uint32) {
	d.mutex.Lock()
	delete(d.data, key)
	delete(d.counts, key)
	d.mutex.Unlock()
}

func (d Data) Dump() string {
	return fmt.Sprintf("ValueMap: %v | CountMap: %v", d.data, d.counts)
}
