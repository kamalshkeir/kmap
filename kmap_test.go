package kmap

import (
	"fmt"
	"testing"
)

type User struct {
	ID   int
	Name string
}

func TestSafeMap(t *testing.T) {
	m := New[int, User](false)

	// Test Set and Get
	m.Set(1, User{1, "John"})
	m.Set(2, User{2, "Jane"})
	if v, ok := m.Get(1); !ok || v.Name != "John" {
		t.Errorf("Error in Set/Get")
	}
	if v, ok := m.Get(3); ok || v.Name != "" {
		t.Errorf("Error in Get")
	}

	// Test Len
	if m.Len() != 2 {
		t.Errorf("Error in Len")
	}

	// Test Keys and Values
	keys := m.Keys()
	values := m.Values()
	if len(keys) != 2 || len(values) != 2 {
		t.Errorf("Error in Keys/Values")
	}

	// Test Delete
	m.Delete(1)
	if m.Len() != 1 {
		t.Errorf("Error in Delete")
	}

	// Test Flush
	m.Flush()
	if m.Len() != 0 {
		t.Errorf("Error in Flush")
	}
}

func BenchmarkSafeMapSet(b *testing.B) {
	m := New[int, User](false)
	for i := 0; i < b.N; i++ {
		m.Set(i, User{i, fmt.Sprintf("User %d", i)})
	}
}

func BenchmarkSafeMapGet(b *testing.B) {
	m := New[int, User](false)
	for i := 0; i < b.N; i++ {
		m.Set(i, User{i, fmt.Sprintf("User %d", i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(i)
	}
}

func BenchmarkSafeMapKeys(b *testing.B) {
	m := New[int, User](false)
	for i := 0; i < b.N; i++ {
		m.Set(i, User{i, fmt.Sprintf("User %d", i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Keys()
	}
}

func BenchmarkSafeMapValues(b *testing.B) {
	m := New[int, User](false)
	for i := 0; i < b.N; i++ {
		m.Set(i, User{i, fmt.Sprintf("User %d", i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Values()
	}
}

func BenchmarkSafeMapDelete(b *testing.B) {
	m := New[int, User](false)
	for i := 0; i < b.N; i++ {
		m.Set(i, User{i, fmt.Sprintf("User %d", i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Delete(i)
	}
}

func BenchmarkSafeMapRange(b *testing.B) {
	m := New[int, User](false)
	for i := 0; i < b.N; i++ {
		m.Set(i, User{i, fmt.Sprintf("User %d", i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Range(func(key int, value User) {})
	}
}
