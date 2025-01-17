package kmap

import (
	"errors"
	"sync"
	"unsafe"
)

var (
	ErrLimitExceeded = errors.New("map exceeds the limit, should be Flushed")
	ErrLargeData     = errors.New("data exceeds the limit limit, will not be inserted")
)

type item[V any] struct {
	Value V
	Size  int
}

type SafeMap[K comparable, V any] struct {
	sync.RWMutex
	items map[K]item[V]
	size  int
	limit int
}

func New[K comparable, V any](limitMb ...int) *SafeMap[K, V] {
	limitmb := -1
	if len(limitMb) > 0 && limitMb[0] > 0 {
		limitmb = limitMb[0] * 1024 * 1024
	}
	return &SafeMap[K, V]{
		items: make(map[K]item[V]),
		size:  0,
		limit: limitmb,
	}
}

func (c *SafeMap[K, V]) Get(key K) (v V, ok bool) {
	c.RLock()
	if i, exists := c.items[key]; exists {
		c.RUnlock()
		return i.Value, true
	}
	c.RUnlock()
	return
}

func (c *SafeMap[K, V]) GetAny(keys ...K) (v V, ok bool) {
	c.RLock()
	for _, key := range keys {
		if i, exists := c.items[key]; exists {
			c.RUnlock()
			return i.Value, true
		}
	}
	c.RUnlock()
	return
}

func getValueSize(value any) int {
	var size int
	switch v := value.(type) {
	case string:
		size = len(v)
	case []byte:
		size = len(v)
	case []int:
		size = len(v) * 8
	case []uint:
		size = len(v) * 8
	case []int64:
		size = len(v) * 8
	case []uint64:
		size = len(v) * 8
	case []float64:
		size = len(v) * 8
	case []float32:
		size = len(v) * 8
	case map[string]string:
		size = 0
		for k, val := range v {
			size += len(k) + len(val)
		}
	case map[string]any:
		size = 0
		for k, val := range v {
			size += len(k)
			if str, ok := val.(string); ok {
				size += len(str)
			} else {
				size += int(unsafe.Sizeof(val))
			}
		}
	case int, uint, int64, uint64, int8, uint8, int16, uint16, int32, uint32:
		size = 8
	default:
		// For basic types and structs, use unsafe.Sizeof
		size = int(unsafe.Sizeof(value))
	}
	return size
}

func (c *SafeMap[K, V]) Set(key K, value V) error {
	c.Lock()
	defer c.Unlock()

	// Check size limits if enabled
	if c.limit > 0 {
		size := getValueSize(value)
		if size > c.limit {
			return ErrLargeData
		}

		if size+c.size > c.limit {
			return ErrLimitExceeded
		}
		// Update size tracking
		if i, exists := c.items[key]; exists {
			c.size -= i.Size
		}

		// Store item in map
		c.items[key] = item[V]{Value: value, Size: size}
		c.size += size

		return nil
	}
	// Store item in map
	c.items[key] = item[V]{Value: value}
	return nil
}

func (c *SafeMap[K, V]) Delete(key K) {
	c.Lock()
	if i, ok := c.items[key]; ok {
		c.size -= i.Size
		delete(c.items, key)
	}
	c.Unlock()
}

func (c *SafeMap[K, V]) Flush() {
	c.Lock()
	if len(c.items) > 0 {
		c.items = make(map[K]item[V])
		c.size = 0
	}
	c.Unlock()
}
func (c *SafeMap[K, V]) Clear() {
	c.Lock()
	if len(c.items) > 0 {
		c.items = make(map[K]item[V])
		c.size = 0
	}
	c.Unlock()
}

func (c *SafeMap[K, V]) Len() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.items)
}

func (c *SafeMap[K, V]) Keys() []K {
	c.RLock()
	n := len(c.items)
	if n == 0 {
		c.RUnlock()
		return nil
	}
	keys := make([]K, n)
	i := 0
	for k := range c.items {
		keys[i] = k
		i++
	}
	c.RUnlock()
	return keys
}

func (c *SafeMap[K, V]) Values() []V {
	c.RLock()
	n := len(c.items)
	if n == 0 {
		c.RUnlock()
		return nil
	}
	values := make([]V, n)
	i := 0
	for _, item := range c.items {
		values[i] = item.Value
		i++
	}
	c.RUnlock()
	return values
}

// Range calls f sequentially for each key and value present in the map. If f returns false, range stops the iteration.
func (c *SafeMap[K, V]) Range(f func(key K, value V) bool) {
	c.RLock()
	// Pre-allocate a slice to avoid map iteration during callback
	n := len(c.items)
	if n == 0 {
		c.RUnlock()
		return
	}
	pairs := make([]struct {
		k K
		v V
	}, 0, n)
	for k, item := range c.items {
		pairs = append(pairs, struct {
			k K
			v V
		}{k, item.Value})
	}
	c.RUnlock()

	// Process items without holding the lock
	for _, p := range pairs {
		if !f(p.k, p.v) {
			break
		}
	}
}

// GetOrSet returns the existing value for the key if present.
// Otherwise, it sets and returns the given value.
func (c *SafeMap[K, V]) GetOrSet(key K, value V) (actual V, loaded bool) {
	if v, ok := c.Get(key); ok {
		return v, true
	}
	c.Set(key, value)
	return value, false
}

// GetOrCompute returns the existing value for the key if present.
// Otherwise, it computes the value using the provided function,
// sets it under the key, and returns the computed value.
func (c *SafeMap[K, V]) GetOrCompute(key K, fn func() V) V {
	if v, ok := c.Get(key); ok {
		return v
	}
	value := fn()
	c.Set(key, value)
	return value
}

// SetIfNotExists sets the value if the key doesn't exist and returns true.
// If the key exists, it returns false and makes no changes.
func (c *SafeMap[K, V]) SetIfNotExists(key K, value V) bool {
	c.Lock()
	if _, exists := c.items[key]; exists {
		c.Unlock()
		return false
	}
	c.Unlock()

	c.Set(key, value)
	return true
}

// DeleteAll removes all the specified keys and returns the number of keys removed
func (c *SafeMap[K, V]) DeleteAll(keys ...K) int {
	if len(keys) == 0 {
		return 0
	}
	c.Lock()
	defer c.Unlock()
	count := 0
	for _, key := range keys {
		if i, ok := c.items[key]; ok {
			c.size -= i.Size
			delete(c.items, key)
			count++
		}
	}
	return count
}

// GetAll returns all the values for the specified keys that exist
func (c *SafeMap[K, V]) GetAll(keys ...K) map[K]V {
	if len(keys) == 0 {
		return nil
	}
	c.RLock()
	result := make(map[K]V, len(keys))
	for _, key := range keys {
		if i, ok := c.items[key]; ok {
			result[key] = i.Value
		}
	}
	c.RUnlock()
	return result
}
