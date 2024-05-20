package kmap

import (
	"errors"
	"reflect"
	"sync"
	"time"
)

var (
	sc           = &sizeCache{cache: make(map[reflect.Type]int)}
	ErrLargeData = errors.New("data exceeds the limit limit, will not be inserted")
)

type item[V any] struct {
	value     V
	timestamp time.Time
	size      int
}

type sizeCache struct {
	sync.RWMutex
	cache map[reflect.Type]int
}

type SafeMap[K comparable, V any] struct {
	sync.RWMutex
	items map[K]*item[V]
	size  int
	limit int
}

func New[K comparable, V any](limitMb ...int) *SafeMap[K, V] {
	limitmb := -1
	if len(limitMb) > 0 && limitMb[0] > 0 {
		limitmb = limitMb[0]
	}
	if limitmb > 1 {
		limitmb = limitmb * 1024 * 1024
	}
	return &SafeMap[K, V]{
		items: make(map[K]*item[V]),
		size:  0,
		limit: limitmb,
	}
}

func (c *SafeMap[K, V]) Get(key K) (V, bool) {
	c.RLock()
	defer c.RUnlock()
	i, ok := c.items[key]
	if !ok {
		return *new(V), false
	}
	return i.value, true
}

func (c *SafeMap[K, V]) GetAny(keys ...K) (V, bool) {
	c.RLock()
	defer c.RUnlock()
	found := false
	for _, key := range keys {
		i, ok := c.items[key]
		if ok {
			found = true
			return i.value, found
		}
	}
	return *new(V), false
}

func (c *SafeMap[K, V]) Set(key K, value V) error {
	c.Lock()
	defer c.Unlock()
	var size int
	if c.limit > 0 {
		size = sc.get(value)
		if size == 0 {
			size = sizeOfValue(value)
			sc.set(value, size)
		}
		if size > c.limit {
			return ErrLargeData
		}

		if size+c.size > c.limit {
			c.items = map[K]*item[V]{}
			c.size = size
		}
	}

	i := &item[V]{
		value:     value,
		timestamp: time.Now(),
		size:      size,
	}
	c.items[key] = i

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
		delete(c.items, key)
	}
}

func (c *SafeMap[K, V]) Flush() {
	c.Lock()
	defer c.Unlock()
	c.items = make(map[K]*item[V])
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

func sizeOfValue(value interface{}) int {
	size := int(reflect.TypeOf(value).Size())
	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		if v.Len() > 0 {
			size = sizeOfValue(v.Index(0).Interface())*v.Len() + size
		}
	case reflect.Map:
		if len(v.MapKeys()) > 0 {
			size = size * len(v.MapKeys())
		}
	case reflect.Struct:
		vnf := v.NumField()
		if vnf > 0 {
			size = size * vnf
		}
	case reflect.Ptr:
		if v.IsNil() {
			return 0
		}
		size += sizeOfValue(v.Elem().Interface())
	}
	return size
}

func (sc *sizeCache) set(value interface{}, size int) {
	sc.Lock()
	defer sc.Unlock()
	t := reflect.TypeOf(value)
	sc.cache[t] = size
}

func (sc *sizeCache) get(value interface{}) int {
	sc.RLock()
	defer sc.RUnlock()
	t := reflect.TypeOf(value)
	size, ok := sc.cache[t]
	if !ok {
		return 0
	}
	return size
}
