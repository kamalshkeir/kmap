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
