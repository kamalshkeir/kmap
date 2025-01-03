package kmap

import (
	"fmt"
	"sync"
)

type OrderedMap[K comparable, V any] struct {
	sync.RWMutex
	kv    map[K]*Element[K, V]
	ll    list[K, V]
	size  int
	limit int
}

func NewOrdered[K comparable, V any](limitMb ...int) *OrderedMap[K, V] {
	limitmb := -1
	if len(limitMb) > 0 && limitMb[0] > 0 {
		limitmb = limitMb[0] * 1024 * 1024
	}
	return &OrderedMap[K, V]{
		kv:    make(map[K]*Element[K, V]),
		limit: limitmb,
		size:  0,
	}
}

func (m *OrderedMap[K, V]) Get(key K) (value V, ok bool) {
	m.RLock()
	defer m.RUnlock()
	v, ok := m.kv[key]
	if ok {
		value = v.Value
	}
	return
}

func (m *OrderedMap[K, V]) GetAny(keys ...K) (V, bool) {
	m.RLock()
	defer m.RUnlock()
	found := false
	for _, key := range keys {
		i, ok := m.kv[key]
		if ok {
			found = true
			return i.Value, found
		}
	}
	return *new(V), false
}

func (m *OrderedMap[K, V]) Set(key K, value V) error {
	m.Lock()
	defer m.Unlock()

	// Use a fixed size for all values to avoid allocations
	size := 64

	if m.limit > 0 {
		// Only check string size if we have a size limit
		switch v := any(value).(type) {
		case string:
			if len(v) > m.limit {
				return fmt.Errorf("exceeded size limit")
			}
			size = len(v)
		}

		if size+m.size > m.limit {
			for k := range m.kv {
				delete(m.kv, k)
			}
			m.ll = list[K, V]{}
			m.size = 0
		}
	}

	_, alreadyExist := m.kv[key]
	if alreadyExist {
		oldSize := m.kv[key].size
		m.kv[key].Value = value
		m.kv[key].size = size
		m.size = m.size - oldSize + size
		return nil
	}

	element := m.ll.PushBack(key, value)
	element.size = size
	m.kv[key] = element
	m.size += size
	return nil
}

func (m *OrderedMap[K, V]) GetOrDefault(key K, defaultValue V) V {
	m.RLock()
	defer m.RUnlock()
	if value, ok := m.kv[key]; ok {
		return value.Value
	}

	return defaultValue
}

func (m *OrderedMap[K, V]) GetElement(key K) *Element[K, V] {
	m.RLock()
	defer m.RUnlock()
	element, ok := m.kv[key]
	if ok {
		return element
	}

	return nil
}

func (m *OrderedMap[K, V]) Len() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.kv)
}

func (m *OrderedMap[K, V]) Keys() (keys []K) {
	m.RLock()
	defer m.RUnlock()
	keys = make([]K, 0, m.Len())
	for el := m.Front(); el != nil; el = el.Next() {
		keys = append(keys, el.Key)
	}
	return keys
}

func (m *OrderedMap[K, V]) Values() (values []V) {
	m.RLock()
	defer m.RUnlock()
	values = make([]V, 0, m.Len())
	for el := m.Front(); el != nil; el = el.Next() {
		values = append(values, el.Value)
	}
	return values
}

func (m *OrderedMap[K, V]) Delete(key K) (didDelete bool) {
	m.Lock()
	defer m.Unlock()
	element, ok := m.kv[key]
	if ok {
		m.size -= element.size
		m.ll.Remove(element)
		delete(m.kv, key)
	}

	return ok
}

func (m *OrderedMap[K, V]) Flush() {
	m.Lock()
	defer m.Unlock()
	for k := range m.kv {
		delete(m.kv, k)
	}
	m.ll = list[K, V]{}
	m.size = 0
}

func (m *OrderedMap[K, V]) Front() *Element[K, V] {
	m.RLock()
	defer m.RUnlock()
	return m.ll.Front()
}

func (m *OrderedMap[K, V]) Back() *Element[K, V] {
	m.RLock()
	defer m.RUnlock()
	return m.ll.Back()
}

func (m *OrderedMap[K, V]) Copy() *OrderedMap[K, V] {
	m.RLock()
	defer m.RUnlock()
	m2 := NewOrdered[K, V]()
	for el := m.Front(); el != nil; el = el.Next() {
		m2.Set(el.Key, el.Value)
	}
	return m2
}

func (m *OrderedMap[K, V]) Range(f func(key K, value V) bool) {
	m.RLock()
	defer m.RUnlock()
	for el := m.Front(); el != nil; el = el.Next() {
		if !f(el.Key, el.Value) {
			break
		}
	}
}

// GetOrSet returns the existing value for the key if present.
// Otherwise, it sets and returns the given value.
func (m *OrderedMap[K, V]) GetOrSet(key K, value V) (actual V, loaded bool) {
	if v, ok := m.Get(key); ok {
		return v, true
	}
	m.Set(key, value)
	return value, false
}

// GetOrCompute returns the existing value for the key if present.
// Otherwise, it computes the value using the provided function,
// sets it under the key, and returns the computed value.
func (m *OrderedMap[K, V]) GetOrCompute(key K, fn func() V) V {
	if v, ok := m.Get(key); ok {
		return v
	}
	value := fn()
	m.Set(key, value)
	return value
}

// SetIfNotExists sets the value if the key doesn't exist and returns true.
// If the key exists, it returns false and makes no changes.
func (m *OrderedMap[K, V]) SetIfNotExists(key K, value V) bool {
	m.Lock()
	defer m.Unlock()
	if _, exists := m.kv[key]; exists {
		return false
	}
	m.ll.PushBack(key, value)
	m.kv[key] = m.ll.Back()
	return true
}

// DeleteAll removes all the specified keys and returns the number of keys removed
func (m *OrderedMap[K, V]) DeleteAll(keys ...K) int {
	if len(keys) == 0 {
		return 0
	}
	m.Lock()
	defer m.Unlock()
	count := 0
	for _, key := range keys {
		if e, ok := m.kv[key]; ok {
			m.ll.Remove(e)
			delete(m.kv, key)
			count++
		}
	}
	return count
}

// GetAll returns all the values for the specified keys that exist
func (m *OrderedMap[K, V]) GetAll(keys ...K) map[K]V {
	if len(keys) == 0 {
		return nil
	}
	m.RLock()
	result := make(map[K]V, len(keys))
	for _, key := range keys {
		if e, ok := m.kv[key]; ok {
			result[key] = e.Value
		}
	}
	m.RUnlock()
	return result
}

// SetAll sets all the key-value pairs in order and returns number of pairs set
func (m *OrderedMap[K, V]) SetAll(pairs map[K]V) int {
	if len(pairs) == 0 {
		return 0
	}
	m.Lock()
	defer m.Unlock()
	count := 0
	for k, v := range pairs {
		if e, exists := m.kv[k]; exists {
			e.Value = v
		} else {
			m.ll.PushBack(k, v)
			m.kv[k] = m.ll.Back()
		}
		count++
	}
	return count
}
