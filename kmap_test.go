package kmap

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

var keyPool = sync.Pool{
	New: func() interface{} {
		return new(strings.Builder)
	},
}

func getKey(i int) string {
	b := keyPool.Get().(*strings.Builder)
	b.Reset()
	b.WriteString("key")
	b.WriteString(fmt.Sprint(i))
	s := b.String()
	keyPool.Put(b)
	return s
}

func TestSafeMap(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		m := New[string, int]()

		// Test Set and Get
		err := m.Set("one", 1)
		if err != nil {
			t.Errorf("Set failed: %v", err)
		}

		val, ok := m.Get("one")
		if !ok || val != 1 {
			t.Errorf("Get failed, expected 1, got %v, ok: %v", val, ok)
		}

		// Test non-existent key
		val, ok = m.Get("two")
		if ok || val != 0 {
			t.Errorf("Get for non-existent key should return zero value and false")
		}

		// Test Delete
		m.Delete("one")
		val, ok = m.Get("one")
		if ok || val != 0 {
			t.Errorf("Delete failed, key still exists")
		}
	})

	t.Run("size limit", func(t *testing.T) {
		m := New[string, string](1) // 1MB limit

		// Test small value
		err := m.Set("small", "hello")
		if err != nil {
			t.Errorf("Set failed for small value: %v", err)
		}

		// Test large value
		largeStr := make([]byte, 2*1024*1024) // 2MB
		t.Logf("Large string size: %d bytes", len(largeStr))
		err = m.Set("large", string(largeStr))
		t.Logf("Error from Set: %v", err)
		if err != ErrLargeData {
			t.Errorf("Expected ErrLargeData for large value, got %v", err)
		}
	})

	t.Run("concurrent operations", func(t *testing.T) {
		m := New[int, int]()
		done := make(chan bool)

		go func() {
			for i := 0; i < 100; i++ {
				m.Set(i, i)
			}
			done <- true
		}()

		go func() {
			for i := 0; i < 100; i++ {
				m.Get(i)
			}
			done <- true
		}()

		<-done
		<-done
	})
}

func TestOrderedMap(t *testing.T) {
	t.Run("ordered operations", func(t *testing.T) {
		m := NewOrdered[string, int]()

		// Test insertion order
		m.Set("one", 1)
		m.Set("two", 2)
		m.Set("three", 3)

		expected := []string{"one", "two", "three"}
		keys := m.Keys()

		if len(keys) != len(expected) {
			t.Errorf("Expected %d keys, got %d", len(expected), len(keys))
		}

		for i, key := range keys {
			if key != expected[i] {
				t.Errorf("Expected key %s at position %d, got %s", expected[i], i, key)
			}
		}
	})

	t.Run("element traversal", func(t *testing.T) {
		m := NewOrdered[string, int]()
		m.Set("first", 1)
		m.Set("second", 2)

		// Test Front()
		front := m.Front()
		if front.Key != "first" || front.Value != 1 {
			t.Errorf("Front() returned wrong element")
		}

		// Test Next()
		second := front.Next()
		if second.Key != "second" || second.Value != 2 {
			t.Errorf("Next() returned wrong element")
		}

		// Test Back()
		back := m.Back()
		if back.Key != "second" || back.Value != 2 {
			t.Errorf("Back() returned wrong element")
		}
	})

	t.Run("copy", func(t *testing.T) {
		m1 := NewOrdered[string, int]()
		m1.Set("one", 1)
		m1.Set("two", 2)

		m2 := m1.Copy()

		if m2.Len() != m1.Len() {
			t.Errorf("Copy length mismatch: expected %d, got %d", m1.Len(), m2.Len())
		}

		val, ok := m2.Get("one")
		if !ok || val != 1 {
			t.Errorf("Copy failed to maintain values")
		}
	})
}

func TestList(t *testing.T) {
	t.Run("basic list operations", func(t *testing.T) {
		l := &list[string, int]{}

		// Test PushBack
		e1 := l.PushBack("one", 1)
		if l.Front() != e1 || l.Back() != e1 {
			t.Error("PushBack failed to set first element correctly")
		}

		// Test PushFront
		e2 := l.PushFront("two", 2)
		if l.Front() != e2 || l.Back() != e1 {
			t.Error("PushFront failed to set element correctly")
		}

		// Test Remove
		l.Remove(e1)
		if l.Back() == e1 {
			t.Error("Remove failed to remove element")
		}
	})
}

func TestGetAny(t *testing.T) {
	t.Run("SafeMap GetAny", func(t *testing.T) {
		m := New[string, int]()
		m.Set("one", 1)
		m.Set("two", 2)

		val, ok := m.GetAny("missing", "one", "two")
		if !ok || val != 1 {
			t.Errorf("GetAny failed to return first matching value")
		}

		val, ok = m.GetAny("missing")
		if ok || val != 0 {
			t.Errorf("GetAny should return false for non-existent keys")
		}
	})

	t.Run("OrderedMap GetAny", func(t *testing.T) {
		m := NewOrdered[string, int]()
		m.Set("one", 1)
		m.Set("two", 2)

		val, ok := m.GetAny("missing", "one", "two")
		if !ok || val != 1 {
			t.Errorf("GetAny failed to return first matching value")
		}

		val, ok = m.GetAny("missing")
		if ok || val != 0 {
			t.Errorf("GetAny should return false for non-existent keys")
		}
	})
}

func BenchmarkSafeMap_SetSmallValues(b *testing.B) {
	m := New[string, string]()
	m.items = make(map[string]item[string], b.N)
	key := "test"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(key, "small value")
	}
}

func BenchmarkSafeMap_SetLargeValues(b *testing.B) {
	m := New[string, string]()
	m.items = make(map[string]item[string], b.N)
	largeValue := strings.Repeat("x", 1024*1024) // 1MB string
	key := "test"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(key, largeValue)
	}
}

func BenchmarkSafeMap_GetExistingKey(b *testing.B) {
	m := New[string, string]()
	m.Set("test", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get("test")
	}
}

func BenchmarkSafeMap_GetNonExistentKey(b *testing.B) {
	m := New[string, string]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get("non-existent")
	}
}

func BenchmarkSafeMap_Delete(b *testing.B) {
	m := New[string, string]()
	m.items = make(map[string]item[string], b.N)
	key := "test"
	for i := 0; i < b.N; i++ {
		m.Set(key, "value")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Delete(key)
	}
}

func BenchmarkSafeMap_GetAnyWithHit(b *testing.B) {
	m := New[string, string]()
	m.Set("key1", "value1")
	m.Set("key2", "value2")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.GetAny("missing", "key1", "key2")
	}
}

func BenchmarkSafeMap_ConcurrentSetAndGet(b *testing.B) {
	m := New[int, int]()
	m.items = make(map[int]item[int], b.N)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				m.Set(i, i)
			} else {
				m.Get(i - 1)
			}
			i++
		}
	})
}

func BenchmarkSafeMap_SizeLimitedOperations(b *testing.B) {
	m := New[string, string](1) // 1MB limit
	m.items = make(map[string]item[string], b.N)
	smallValue := "small"
	largeValue := strings.Repeat("x", 2*1024*1024) // 2MB string
	key := "test"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			m.Set(key, smallValue)
		} else {
			m.Set(key, largeValue)
		}
	}
}

func BenchmarkOrderedMap_SetAndMaintainOrder(b *testing.B) {
	m := NewOrdered[string, int]()
	m.kv = make(map[string]*Element[string, int], b.N)
	key := "test"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(key, i)
	}
}

func BenchmarkOrderedMap_Traversal(b *testing.B) {
	m := NewOrdered[string, int]()
	m.kv = make(map[string]*Element[string, int], 1000)
	for i := 0; i < 1000; i++ { // Setup with 1000 items
		m.Set(fmt.Sprintf("key%d", i), i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for e := m.Front(); e != nil; e = e.Next() {
			_ = e.Value
		}
	}
}

func BenchmarkOrderedMap_Copy(b *testing.B) {
	m := NewOrdered[string, int]()
	m.kv = make(map[string]*Element[string, int], 1000)
	for i := 0; i < 1000; i++ { // Setup with 1000 items
		m.Set(fmt.Sprintf("key%d", i), i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Copy()
	}
}

func BenchmarkSafeMap_Range(b *testing.B) {
	m := New[string, int]()
	m.items = make(map[string]item[int], 1000)
	for i := 0; i < 1000; i++ {
		m.Set(getKey(i), i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Range(func(key string, value int) bool {
			return true
		})
	}
}

func BenchmarkSafeMap_Flush(b *testing.B) {
	m := New[string, int]()
	m.items = make(map[string]item[int], 100)
	// Pre-fill the map
	for i := 0; i < 100; i++ {
		m.Set(fmt.Sprintf("key%d", i), i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Flush()
	}
}

func BenchmarkSafeMap_Keys(b *testing.B) {
	m := New[string, int]()
	m.items = make(map[string]item[int], 1000)
	for i := 0; i < 1000; i++ {
		m.Set(getKey(i), i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Keys()
	}
}

func BenchmarkSafeMap_Values(b *testing.B) {
	m := New[string, int]()
	m.items = make(map[string]item[int], 1000)
	for i := 0; i < 1000; i++ {
		m.Set(getKey(i), i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Values()
	}
}

func BenchmarkSafeMap_KeysEmpty(b *testing.B) {
	m := New[string, int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Keys()
	}
}

func BenchmarkSafeMap_ValuesEmpty(b *testing.B) {
	m := New[string, int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Values()
	}
}

type stdSafeMap struct {
	sync.RWMutex
	items map[string]any
}

func newStdMap() *stdSafeMap {
	return &stdSafeMap{
		items: make(map[string]any),
	}
}

func (m *stdSafeMap) Get(key string) (any, bool) {
	m.RLock()
	v, ok := m.items[key]
	m.RUnlock()
	return v, ok
}

func (m *stdSafeMap) Set(key string, value any) {
	m.Lock()
	m.items[key] = value
	m.Unlock()
}

func BenchmarkStdMap_Get(b *testing.B) {
	m := newStdMap()
	m.Set("test", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get("test")
	}
}

func BenchmarkStdMap_GetNonExistent(b *testing.B) {
	m := newStdMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get("non-existent")
	}
}

func BenchmarkStdMap_Set(b *testing.B) {
	m := newStdMap()
	m.items = make(map[string]any, b.N)
	key := "test"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(key, "small value")
	}
}

func BenchmarkStdMap_SetLarge(b *testing.B) {
	m := newStdMap()
	m.items = make(map[string]any, b.N)
	largeValue := strings.Repeat("x", 1024*1024) // 1MB string
	key := "test"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(key, largeValue)
	}
}

func BenchmarkStdMap_ConcurrentSetAndGet(b *testing.B) {
	m := newStdMap()
	m.items = make(map[string]any, b.N)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				m.Set(fmt.Sprint(i), i)
			} else {
				m.Get(fmt.Sprint(i - 1))
			}
			i++
		}
	})
}

func TestSafeMap_Sugar(t *testing.T) {
	t.Run("GetOrSet", func(t *testing.T) {
		m := New[string, int]()

		// Test getting non-existent value
		val, loaded := m.GetOrSet("key", 42)
		if loaded || val != 42 {
			t.Errorf("GetOrSet should set new value, got loaded=%v, val=%v", loaded, val)
		}

		// Test getting existing value
		val, loaded = m.GetOrSet("key", 100)
		if !loaded || val != 42 {
			t.Errorf("GetOrSet should return existing value, got loaded=%v, val=%v", loaded, val)
		}
	})

	t.Run("GetOrCompute", func(t *testing.T) {
		m := New[string, int]()
		computed := false

		fn := func() int {
			computed = true
			return 42
		}

		// Test computing non-existent value
		val := m.GetOrCompute("key", fn)
		if !computed || val != 42 {
			t.Errorf("GetOrCompute should compute new value, got computed=%v, val=%v", computed, val)
		}

		// Test getting existing value
		computed = false
		val = m.GetOrCompute("key", fn)
		if computed || val != 42 {
			t.Errorf("GetOrCompute should return existing value, got computed=%v, val=%v", computed, val)
		}
	})

	t.Run("SetIfNotExists", func(t *testing.T) {
		m := New[string, int]()

		// Test setting non-existent value
		if !m.SetIfNotExists("key", 42) {
			t.Error("SetIfNotExists should return true for new key")
		}

		// Test setting existing value
		if m.SetIfNotExists("key", 100) {
			t.Error("SetIfNotExists should return false for existing key")
		}

		// Verify value wasn't changed
		if val, _ := m.Get("key"); val != 42 {
			t.Errorf("SetIfNotExists should not change existing value, got %v", val)
		}
	})

	t.Run("DeleteAll", func(t *testing.T) {
		m := New[string, int]()
		m.Set("one", 1)
		m.Set("two", 2)
		m.Set("three", 3)

		count := m.DeleteAll("one", "two", "missing")
		if count != 2 {
			t.Errorf("DeleteAll should return number of deleted keys, got %d", count)
		}

		if m.Len() != 1 {
			t.Errorf("DeleteAll should remove specified keys, got len=%d", m.Len())
		}
	})

	t.Run("GetAll", func(t *testing.T) {
		m := New[string, int]()
		m.Set("one", 1)
		m.Set("two", 2)

		result := m.GetAll("one", "two", "missing")
		if len(result) != 2 {
			t.Errorf("GetAll should return only existing keys, got %d keys", len(result))
		}
		if result["one"] != 1 || result["two"] != 2 {
			t.Error("GetAll returned incorrect values")
		}
	})
}

func TestOrderedMap_Sugar(t *testing.T) {
	t.Run("GetOrSet", func(t *testing.T) {
		m := NewOrdered[string, int]()

		// Test getting non-existent value
		val, loaded := m.GetOrSet("key", 42)
		if loaded || val != 42 {
			t.Errorf("GetOrSet should set new value, got loaded=%v, val=%v", loaded, val)
		}

		// Test getting existing value
		val, loaded = m.GetOrSet("key", 100)
		if !loaded || val != 42 {
			t.Errorf("GetOrSet should return existing value, got loaded=%v, val=%v", loaded, val)
		}
	})

	t.Run("GetOrCompute", func(t *testing.T) {
		m := NewOrdered[string, int]()
		computed := false

		fn := func() int {
			computed = true
			return 42
		}

		// Test computing non-existent value
		val := m.GetOrCompute("key", fn)
		if !computed || val != 42 {
			t.Errorf("GetOrCompute should compute new value, got computed=%v, val=%v", computed, val)
		}

		// Test getting existing value
		computed = false
		val = m.GetOrCompute("key", fn)
		if computed || val != 42 {
			t.Errorf("GetOrCompute should return existing value, got computed=%v, val=%v", computed, val)
		}
	})

	t.Run("SetIfNotExists", func(t *testing.T) {
		m := NewOrdered[string, int]()

		// Test setting non-existent value
		if !m.SetIfNotExists("key", 42) {
			t.Error("SetIfNotExists should return true for new key")
		}

		// Test setting existing value
		if m.SetIfNotExists("key", 100) {
			t.Error("SetIfNotExists should return false for existing key")
		}

		// Verify value wasn't changed
		if val, _ := m.Get("key"); val != 42 {
			t.Errorf("SetIfNotExists should not change existing value, got %v", val)
		}
	})

	t.Run("DeleteAll", func(t *testing.T) {
		m := NewOrdered[string, int]()
		m.Set("one", 1)
		m.Set("two", 2)
		m.Set("three", 3)

		count := m.DeleteAll("one", "two", "missing")
		if count != 2 {
			t.Errorf("DeleteAll should return number of deleted keys, got %d", count)
		}

		if m.Len() != 1 {
			t.Errorf("DeleteAll should remove specified keys, got len=%d", m.Len())
		}
	})

	t.Run("GetAll", func(t *testing.T) {
		m := NewOrdered[string, int]()
		m.Set("one", 1)
		m.Set("two", 2)

		result := m.GetAll("one", "two", "missing")
		if len(result) != 2 {
			t.Errorf("GetAll should return only existing keys, got %d keys", len(result))
		}
		if result["one"] != 1 || result["two"] != 2 {
			t.Error("GetAll returned incorrect values")
		}
	})
}
