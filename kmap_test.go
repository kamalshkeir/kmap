package kmap

import (
	"fmt"
	"testing"
)

type User struct {
	ID   int
	Name string
}

var bigSlice = make([]User, 400)
var safe2 = New[int, any](false, 500)

func init() {
	for i := 0; i < 400; i++ {
		bigSlice[i] = User{i, fmt.Sprintf("User %d", i)}
	}
}

func TestSafeMap(t *testing.T) {
	m := New[int, User](false, 10)

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

func TestSafeMapOrdered(t *testing.T) {
	m := New[string, User](true, 10)

	// Test Set and Get
	m.Set("1", User{1, "Jane1"})
	m.Set("2", User{2, "Jane2"})
	m.Set("3", User{3, "Jane3"})
	m.Set("4", User{4, "Jane4"})
	keys := m.Keys()
	if len(keys) != 4 || keys[3] != "4" {
		t.Error("order not working for keys", keys)
	}
	values := m.Values()
	if len(values) != 4 || values[3].Name != "Jane4" {
		t.Error("order not working for values", values)
	}
}

func TestSafeMapLimit(t *testing.T) {
	m := New[int, []byte](false, 11)
	data := make([]byte, 10*1024*1024)
	// Test Set and Get
	err := m.Set(1, data)
	if err != nil {
		t.Error(err)
	}
	err = m.Set(2, data)
	if err != nil {
		t.Error(err)
	}

	d, ok := m.Get(1)
	if !ok {
		t.Error("not okk")
	}
	t.Log(len(d))
}

func BenchmarkSafeMapSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		safe2.Set(i, bigSlice)
		// if err != nil {
		// 	b.Log(err)
		// }
	}
}

func BenchmarkSafeMapGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		safe2.Get(i)
	}
}

func BenchmarkSafeMapKeys(b *testing.B) {
	m := New[int, User](false, 50)
	for i := 0; i < b.N; i++ {
		m.Set(i, User{i, fmt.Sprintf("User %d", i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Keys()
	}
}

func BenchmarkSafeMapValues(b *testing.B) {
	m := New[int, User](false, 100)
	for i := 0; i < b.N; i++ {
		m.Set(i, User{i, fmt.Sprintf("User %d", i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Values()
	}
}

func BenchmarkSafeMapDelete(b *testing.B) {
	m := New[int, User](false, 100)
	for i := 0; i < b.N; i++ {
		m.Set(i, User{i, fmt.Sprintf("User %d", i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Delete(i)
	}
}

func BenchmarkSafeMapRange(b *testing.B) {
	m := New[int, User](false, 50)
	for i := 0; i < b.N; i++ {
		m.Set(i, User{i, fmt.Sprintf("User %d", i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Range(func(key int, value User) {})
	}
}
