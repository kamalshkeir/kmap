package kmap

import (
	"errors"
	"sync"
)

var (
	ErrLargeData = errors.New("data exceeds the limit limit, will not be inserted")
)

type item[V any] struct {
	value V
	size  int
}

func (i *item[V]) reset(value V, size int) {
	i.value = value
	i.size = size
}

type itemPool[V any] struct {
	pool sync.Pool
}

func newItemPool[V any]() *itemPool[V] {
	return &itemPool[V]{
		pool: sync.Pool{
			New: func() interface{} {
				return new(item[V])
			},
		},
	}
}

func (p *itemPool[V]) get() *item[V] {
	return p.pool.Get().(*item[V])
}

func (p *itemPool[V]) put(i *item[V]) {
	p.pool.Put(i)
}

type SafeMap[K comparable, V any] struct {
	sync.RWMutex
	items map[K]*item[V]
	size  int
	limit int
	pool  *itemPool[V]
}

func New[K comparable, V any](limitMb ...int) *SafeMap[K, V] {
	limitmb := -1
	if len(limitMb) > 0 && limitMb[0] > 0 {
		limitmb = limitMb[0] * 1024 * 1024
	}
	return &SafeMap[K, V]{
		items: make(map[K]*item[V]),
		size:  0,
		limit: limitmb,
		pool:  newItemPool[V](),
	}
}

func (c *SafeMap[K, V]) Get(key K) (v V, ok bool) {
	c.RLock()
	if i, exists := c.items[key]; exists {
		c.RUnlock()
		return i.value, true
	}
	c.RUnlock()
	return
}

func (c *SafeMap[K, V]) GetAny(keys ...K) (v V, ok bool) {
	c.RLock()
	for _, key := range keys {
		if i, exists := c.items[key]; exists {
			c.RUnlock()
			return i.value, true
		}
	}
	c.RUnlock()
	return
}

func (c *SafeMap[K, V]) Set(key K, value V) error {
	c.Lock()
	defer c.Unlock()

	// Use a fixed size for all values to avoid allocations
	size := 64

	// Check size limits if enabled
	if c.limit > 0 {
		// Only check string size if we have a size limit
		switch v := any(value).(type) {
		case string:
			if len(v) > c.limit {
				return ErrLargeData
			}
			size = len(v)
		}

		if size+c.size > c.limit {
			// Clear map and return items to pool
			for _, oldItem := range c.items {
				c.pool.put(oldItem)
			}
			for k := range c.items {
				delete(c.items, k)
			}
			c.size = 0
		}
	}

	// Get existing item or get one from pool
	i, exists := c.items[key]
	if !exists {
		i = c.pool.get()
	} else {
		c.size -= i.size // Subtract old size
	}

	i.reset(value, size)
	c.items[key] = i
	c.size += size

	return nil
}

func (c *SafeMap[K, V]) Len() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.items)
}

func (c *SafeMap[K, V]) Keys() []K {
	c.RLock()
	defer c.RUnlock()
	keys := make([]K, 0, len(c.items))
	for key := range c.items {
		keys = append(keys, key)
	}
	return keys
}

func (c *SafeMap[K, V]) Values() []V {
	c.RLock()
	defer c.RUnlock()
	values := make([]V, 0, len(c.items))
	for _, item := range c.items {
		values = append(values, item.value)
	}
	return values
}

func (c *SafeMap[K, V]) Delete(key K) {
	c.Lock()
	defer c.Unlock()
	i, ok := c.items[key]
	if ok {
		c.size -= i.size
		c.pool.put(i)
		delete(c.items, key)
	}
}

func (c *SafeMap[K, V]) Flush() {
	c.Lock()
	defer c.Unlock()
	// Return all items to pool
	for _, item := range c.items {
		c.pool.put(item)
	}
	for k := range c.items {
		delete(c.items, k)
	}
	c.size = 0
}

// Range calls f sequentially for each key and value present in the map. If f returns false, range stops the iteration.
func (c *SafeMap[K, V]) Range(f func(key K, value V) bool) {
	c.RLock()
	defer c.RUnlock()
	for k, v := range c.items {
		if !f(k, v.value) {
			break
		}
	}
}
