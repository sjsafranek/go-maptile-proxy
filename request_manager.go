package main

import (
	"sync"
)

// ---- request coalescing (minimal singleflight) ----

type request struct {
	waitGroup sync.WaitGroup
	err       error
}

type requestManager struct {
	mutex    sync.Mutex
	requests map[string]*request
}

func (self *requestManager) Do(key string, callback func() error) error {
	self.mutex.Lock()
	if req, ok := self.requests[key]; ok {
		self.mutex.Unlock()
		req.waitGroup.Wait()
		return req.err
	}
	req := &request{}
	req.waitGroup.Add(1)
	self.requests[key] = req
	self.mutex.Unlock()

	req.err = callback()
	req.waitGroup.Done()

	self.mutex.Lock()
	delete(self.requests, key)
	self.mutex.Unlock()
	return req.err
}

func (self *requestManager) Len() int {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return len(self.requests)
}

func newRequestManager() requestManager {
	return requestManager{requests: make(map[string]*request)}
}
