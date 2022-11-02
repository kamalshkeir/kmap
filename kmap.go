package kmap

import (
	"sync"
)

type SafeMap[K comparable, V any] struct {
	m       map[K]V
	mutex   sync.RWMutex
	order   map[int]K
	ordered bool
}

func New[K comparable, V any](oredered bool) *SafeMap[K, V] {
	var order =map[int]K{}
	return &SafeMap[K, V]{
		mutex: sync.RWMutex{},
		m:     make(map[K]V),
		order: order,
		ordered: oredered,
	}
}

func (sm *SafeMap[K, V]) Get(key K) (V, bool) {
	sm.mutex.RLock()
	if v, ok := sm.m[key]; ok {
		sm.mutex.RUnlock()
		return v, true
	}
	sm.mutex.RUnlock()
	return *new(V), false
}

func (sm *SafeMap[K, V]) Set(key K, value V) {
	sm.mutex.Lock()
	sm.m[key] = value
	sm.order[sm.Len()] = key
	sm.mutex.Unlock()
}

func (sm *SafeMap[K, V]) Len() int {
	return len(sm.m)
}

func (sm *SafeMap[K, V]) Keys() []K {
	l := make([]K, len(sm.m))
	i := 0
	sm.mutex.RLock()
	if sm.ordered {
		for range sm.m {
			l[i] = sm.order[i]
			i++
		}
	} else {
		for k := range sm.m {
			l[i] = k
			i++
		}
	}
	sm.mutex.RUnlock()
	return l
}

func (sm *SafeMap[K, V]) Values() []V {
	l := make([]V, len(sm.m))
	i := 0
	sm.mutex.RLock()
	for _, v := range sm.m {
		l[i] = v
		i++
	}
	sm.mutex.RUnlock()
	return l
}

func (sm *SafeMap[K, V]) Delete(key K) {
	sm.mutex.Lock()
	delete(sm.m, key)
	sm.mutex.Unlock()
}

func (sm *SafeMap[K, V]) Flush() {
	sm.mutex.Lock()
	sm.m = map[K]V{}
	sm.mutex.Unlock()
}


func (sm *SafeMap[K, V]) Range(fn func(key K, value V)) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	for k, v := range sm.m {
		fn(k, v)
	}
}
