package main

import (
	"sync"
	"time"
)

type Create func(interface{}) interface{}

type Cache struct {
	m sync.Map
	// create lock, if Get miss, aquire the lock, then call Create, protect duplicate call Create func
	l sync.Mutex
	// record each key's expire time
	t sync.Map
	// clean worker check interval
	i time.Duration
}

func NewCache(interval time.Duration) *Cache {
	c := Cache{
		i: interval,
	}
	go c.cleanWorker()
	return &c
}

func (c *Cache) cleanWorker() {
	for {
		now := <-time.After(c.i)
		c.t.Range(func(key, value interface{}) bool {
			if t := value.(time.Time); t.Before(now) {
				log("DELETE CACHE", key)
				c.m.Delete(key)
				c.t.Delete(key)
			}
			return true
		})
	}
}

/// if key not set yet, will create it, and delete the key after ttl
/// To disable auto delete, make ttl < 0
func (c *Cache) Get(key interface{}, create Create, ttl time.Duration) (interface{}, error) {
	v, ok := c.m.Load(key)
	if ok {
		return v, nil
	}

	c.l.Lock()
	// double check
	v, ok = c.m.Load(key)
	if ok {
		c.l.Unlock()
		return v, nil
	}
	// create and store
	v = create(key)
	c.m.Store(key, v)
	c.l.Unlock()
	log("CREATE CACHE", key)

	// if ttl >= 0, record expire time for delete
	if ttl >= 0 {
		c.t.Store(key, time.Now().Add(ttl))
	}

	return v, nil
}
