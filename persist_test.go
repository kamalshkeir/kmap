package kmap

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSafeMap_Persistence(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "kmap_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("SaveAndLoad", func(t *testing.T) {
		m1 := New[string, int](1) // 1MB limit
		m1.Set("one", 1)
		m1.Set("two", 2)
		m1.Set("three", 3)

		// Save the map
		path := filepath.Join(tmpDir, "safemap.bin")
		if err := m1.SaveToFile(path); err != nil {
			t.Fatalf("Failed to save map: %v", err)
		}

		// Load into a new map
		m2 := New[string, int]()
		if err := m2.LoadFromFile(path); err != nil {
			t.Fatalf("Failed to load map: %v", err)
		}

		// Verify contents
		if m2.Len() != m1.Len() {
			t.Errorf("Loaded map has wrong length: got %d, want %d", m2.Len(), m1.Len())
		}

		for k, v := range map[string]int{"one": 1, "two": 2, "three": 3} {
			if val, ok := m2.Get(k); !ok || val != v {
				t.Errorf("Wrong value for key %q: got %v, want %v", k, val, v)
			}
		}

		// Verify size limit was preserved
		if m2.limit != 1024*1024 {
			t.Errorf("Size limit not preserved: got %d, want %d", m2.limit, 1024*1024)
		}
	})

	t.Run("LoadNonExistentFile", func(t *testing.T) {
		m := New[string, int]()
		err := m.LoadFromFile(filepath.Join(tmpDir, "nonexistent.bin"))
		if err == nil {
			t.Error("Expected error when loading non-existent file")
		}
	})

	t.Run("SaveWithStringValues", func(t *testing.T) {
		m1 := New[string, string](1) // 1MB limit
		m1.Set("key1", "small value")
		m1.Set("key2", "another value")

		// Save the map
		path := filepath.Join(tmpDir, "safemap_strings.bin")
		if err := m1.SaveToFile(path); err != nil {
			t.Fatalf("Failed to save map: %v", err)
		}

		// Load into a new map
		m2 := New[string, string]()
		if err := m2.LoadFromFile(path); err != nil {
			t.Fatalf("Failed to load map: %v", err)
		}

		// Verify contents and size tracking
		if m2.size != m1.size {
			t.Errorf("Size tracking not preserved: got %d, want %d", m2.size, m1.size)
		}

		v1, _ := m1.Get("key1")
		v2, _ := m2.Get("key1")
		if v1 != v2 {
			t.Errorf("String value not preserved: got %q, want %q", v2, v1)
		}
	})
}

func TestOrderedMap_Persistence(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "kmap_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("SaveAndLoad", func(t *testing.T) {
		m1 := NewOrdered[string, int](1) // 1MB limit
		m1.Set("one", 1)
		m1.Set("two", 2)
		m1.Set("three", 3)

		// Save the map
		path := filepath.Join(tmpDir, "orderedmap.bin")
		if err := m1.SaveToFile(path); err != nil {
			t.Fatalf("Failed to save map: %v", err)
		}

		// Load into a new map
		m2 := NewOrdered[string, int]()
		if err := m2.LoadFromFile(path); err != nil {
			t.Fatalf("Failed to load map: %v", err)
		}

		// Verify contents
		if m2.Len() != m1.Len() {
			t.Errorf("Loaded map has wrong length: got %d, want %d", m2.Len(), m1.Len())
		}

		// Verify order is preserved
		keys1 := m1.Keys()
		keys2 := m2.Keys()
		if len(keys1) != len(keys2) {
			t.Fatalf("Key length mismatch: got %d, want %d", len(keys2), len(keys1))
		}
		for i := range keys1 {
			if keys1[i] != keys2[i] {
				t.Errorf("Key order not preserved at position %d: got %v, want %v", i, keys2[i], keys1[i])
			}
		}

		// Verify values
		for k, v := range map[string]int{"one": 1, "two": 2, "three": 3} {
			if val, ok := m2.Get(k); !ok || val != v {
				t.Errorf("Wrong value for key %q: got %v, want %v", k, val, v)
			}
		}

		// Verify size limit was preserved
		if m2.limit != 1024*1024 {
			t.Errorf("Size limit not preserved: got %d, want %d", m2.limit, 1024*1024)
		}
	})

	t.Run("LoadNonExistentFile", func(t *testing.T) {
		m := NewOrdered[string, int]()
		err := m.LoadFromFile(filepath.Join(tmpDir, "nonexistent.bin"))
		if err == nil {
			t.Error("Expected error when loading non-existent file")
		}
	})

	t.Run("SaveWithStringValues", func(t *testing.T) {
		m1 := NewOrdered[string, string](1) // 1MB limit
		m1.Set("key1", "small value")
		m1.Set("key2", "another value")

		// Save the map
		path := filepath.Join(tmpDir, "orderedmap_strings.bin")
		if err := m1.SaveToFile(path); err != nil {
			t.Fatalf("Failed to save map: %v", err)
		}

		// Load into a new map
		m2 := NewOrdered[string, string]()
		if err := m2.LoadFromFile(path); err != nil {
			t.Fatalf("Failed to load map: %v", err)
		}

		// Verify contents and size tracking
		if m2.size != m1.size {
			t.Errorf("Size tracking not preserved: got %d, want %d", m2.size, m1.size)
		}

		// Verify order and values
		keys1 := m1.Keys()
		keys2 := m2.Keys()
		for i := range keys1 {
			v1, _ := m1.Get(keys1[i])
			v2, _ := m2.Get(keys2[i])
			if v1 != v2 {
				t.Errorf("Value not preserved for key %q: got %q, want %q", keys1[i], v2, v1)
			}
		}
	})
}

func BenchmarkSafeMap_Persistence(b *testing.B) {
	// Create a temporary directory for benchmark files
	tmpDir, err := os.MkdirTemp("", "kmap_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	b.Run("SaveSmallMap", func(b *testing.B) {
		m := New[string, int](1)
		for i := 0; i < 100; i++ {
			m.Set(fmt.Sprint(i), i)
		}
		path := filepath.Join(tmpDir, "bench_small.bin")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := m.SaveToFile(path); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("LoadSmallMap", func(b *testing.B) {
		// Create a map and save it first
		m1 := New[string, int](1)
		for i := 0; i < 100; i++ {
			m1.Set(fmt.Sprint(i), i)
		}
		path := filepath.Join(tmpDir, "bench_small_load.bin")
		if err := m1.SaveToFile(path); err != nil {
			b.Fatal(err)
		}

		// Benchmark loading
		m2 := New[string, int]()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := m2.LoadFromFile(path); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SaveLargeMap", func(b *testing.B) {
		m := New[string, int](10)
		for i := 0; i < 10000; i++ {
			m.Set(fmt.Sprint(i), i)
		}
		path := filepath.Join(tmpDir, "bench_large.bin")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := m.SaveToFile(path); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("LoadLargeMap", func(b *testing.B) {
		// Create a map and save it first
		m1 := New[string, int](10)
		for i := 0; i < 10000; i++ {
			m1.Set(fmt.Sprint(i), i)
		}
		path := filepath.Join(tmpDir, "bench_large_load.bin")
		if err := m1.SaveToFile(path); err != nil {
			b.Fatal(err)
		}

		// Benchmark loading
		m2 := New[string, int]()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := m2.LoadFromFile(path); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SaveWithStrings", func(b *testing.B) {
		m := New[string, string](10)
		val := strings.Repeat("x", 1000) // 1KB string
		for i := 0; i < 1000; i++ {
			m.Set(fmt.Sprint(i), val)
		}
		path := filepath.Join(tmpDir, "bench_strings.bin")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := m.SaveToFile(path); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkOrderedMap_Persistence(b *testing.B) {
	// Create a temporary directory for benchmark files
	tmpDir, err := os.MkdirTemp("", "kmap_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	b.Run("SaveSmallMap", func(b *testing.B) {
		m := NewOrdered[string, int](1)
		for i := 0; i < 100; i++ {
			m.Set(fmt.Sprint(i), i)
		}
		path := filepath.Join(tmpDir, "bench_ordered_small.bin")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := m.SaveToFile(path); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("LoadSmallMap", func(b *testing.B) {
		// Create a map and save it first
		m1 := NewOrdered[string, int](1)
		for i := 0; i < 100; i++ {
			m1.Set(fmt.Sprint(i), i)
		}
		path := filepath.Join(tmpDir, "bench_ordered_small_load.bin")
		if err := m1.SaveToFile(path); err != nil {
			b.Fatal(err)
		}

		// Benchmark loading
		m2 := NewOrdered[string, int]()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := m2.LoadFromFile(path); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SaveLargeMap", func(b *testing.B) {
		m := NewOrdered[string, int](10)
		for i := 0; i < 10000; i++ {
			m.Set(fmt.Sprint(i), i)
		}
		path := filepath.Join(tmpDir, "bench_ordered_large.bin")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := m.SaveToFile(path); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("LoadLargeMap", func(b *testing.B) {
		// Create a map and save it first
		m1 := NewOrdered[string, int](10)
		for i := 0; i < 10000; i++ {
			m1.Set(fmt.Sprint(i), i)
		}
		path := filepath.Join(tmpDir, "bench_ordered_large_load.bin")
		if err := m1.SaveToFile(path); err != nil {
			b.Fatal(err)
		}

		// Benchmark loading
		m2 := NewOrdered[string, int]()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := m2.LoadFromFile(path); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SaveWithStrings", func(b *testing.B) {
		m := NewOrdered[string, string](10)
		val := strings.Repeat("x", 1000) // 1KB string
		for i := 0; i < 1000; i++ {
			m.Set(fmt.Sprint(i), val)
		}
		path := filepath.Join(tmpDir, "bench_ordered_strings.bin")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := m.SaveToFile(path); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestSafeMap_AsyncPersistence(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "kmap_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("SaveAndLoadAsync", func(t *testing.T) {
		m1 := New[string, int](1) // 1MB limit
		m1.Set("one", 1)
		m1.Set("two", 2)
		m1.Set("three", 3)

		// Save the map asynchronously
		path := filepath.Join(tmpDir, "safemap_async.bin")
		result := m1.SaveToFileAsync(path)

		// Monitor progress
		var lastProgress int64
		for {
			select {
			case <-result.Done:
				goto SaveDone
			default:
				progress := result.Progress.Load()
				if progress > lastProgress {
					t.Logf("Save progress: %d%%", progress)
					lastProgress = progress
				}
			}
		}
	SaveDone:
		if result.Error != nil {
			t.Fatalf("Failed to save map: %v", result.Error)
		}

		// Load into a new map asynchronously
		m2 := New[string, int]()
		loadResult := m2.LoadFromFileAsync(path)

		// Monitor progress
		lastProgress = 0
		for {
			select {
			case <-loadResult.Done:
				goto LoadDone
			default:
				progress := loadResult.Progress.Load()
				if progress > lastProgress {
					t.Logf("Load progress: %d%%", progress)
					lastProgress = progress
				}
			}
		}
	LoadDone:
		if loadResult.Error != nil {
			t.Fatalf("Failed to load map: %v", loadResult.Error)
		}

		// Verify contents
		if m2.Len() != m1.Len() {
			t.Errorf("Loaded map has wrong length: got %d, want %d", m2.Len(), m1.Len())
		}

		for k, v := range map[string]int{"one": 1, "two": 2, "three": 3} {
			if val, ok := m2.Get(k); !ok || val != v {
				t.Errorf("Wrong value for key %q: got %v, want %v", k, val, v)
			}
		}
	})

	t.Run("SaveLargeMapAsync", func(t *testing.T) {
		m1 := New[string, string](10)         // 10MB limit
		val := strings.Repeat("x", 1024*1024) // 1MB string
		for i := 0; i < 5; i++ {
			m1.Set(fmt.Sprint(i), val)
		}

		// Save the map asynchronously
		path := filepath.Join(tmpDir, "safemap_large_async.bin")
		result := m1.SaveToFileAsync(path)

		// Monitor progress
		var lastProgress int64
		for {
			select {
			case <-result.Done:
				goto Done
			default:
				progress := result.Progress.Load()
				if progress > lastProgress {
					t.Logf("Progress: %d%%", progress)
					lastProgress = progress
				}
			}
		}
	Done:
		if result.Error != nil {
			t.Fatalf("Failed to save large map: %v", result.Error)
		}
	})
}

func TestOrderedMap_AsyncPersistence(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "kmap_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("SaveAndLoadAsync", func(t *testing.T) {
		m1 := NewOrdered[string, int](1) // 1MB limit
		m1.Set("one", 1)
		m1.Set("two", 2)
		m1.Set("three", 3)

		// Save the map asynchronously
		path := filepath.Join(tmpDir, "orderedmap_async.bin")
		result := m1.SaveToFileAsync(path)

		// Monitor progress
		var lastProgress int64
		for {
			select {
			case <-result.Done:
				goto SaveDone
			default:
				progress := result.Progress.Load()
				if progress > lastProgress {
					t.Logf("Save progress: %d%%", progress)
					lastProgress = progress
				}
			}
		}
	SaveDone:
		if result.Error != nil {
			t.Fatalf("Failed to save map: %v", result.Error)
		}

		// Load into a new map asynchronously
		m2 := NewOrdered[string, int]()
		loadResult := m2.LoadFromFileAsync(path)

		// Monitor progress
		lastProgress = 0
		for {
			select {
			case <-loadResult.Done:
				goto LoadDone
			default:
				progress := loadResult.Progress.Load()
				if progress > lastProgress {
					t.Logf("Load progress: %d%%", progress)
					lastProgress = progress
				}
			}
		}
	LoadDone:
		if loadResult.Error != nil {
			t.Fatalf("Failed to load map: %v", loadResult.Error)
		}

		// Verify contents and order
		keys1 := m1.Keys()
		keys2 := m2.Keys()
		if len(keys1) != len(keys2) {
			t.Fatalf("Key length mismatch: got %d, want %d", len(keys2), len(keys1))
		}
		for i := range keys1 {
			if keys1[i] != keys2[i] {
				t.Errorf("Key order not preserved at position %d: got %v, want %v", i, keys2[i], keys1[i])
			}
		}
	})

	t.Run("SaveLargeMapAsync", func(t *testing.T) {
		m1 := NewOrdered[string, string](10)  // 10MB limit
		val := strings.Repeat("x", 1024*1024) // 1MB string
		for i := 0; i < 5; i++ {
			m1.Set(fmt.Sprint(i), val)
		}

		// Save the map asynchronously
		path := filepath.Join(tmpDir, "orderedmap_large_async.bin")
		result := m1.SaveToFileAsync(path)

		// Monitor progress
		var lastProgress int64
		for {
			select {
			case <-result.Done:
				goto Done
			default:
				progress := result.Progress.Load()
				if progress > lastProgress {
					t.Logf("Progress: %d%%", progress)
					lastProgress = progress
				}
			}
		}
	Done:
		if result.Error != nil {
			t.Fatalf("Failed to save large map: %v", result.Error)
		}
	})
}

func TestSafeMap_Compression(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "kmap_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("CompressLargeData", func(t *testing.T) {
		m1 := New[string, string](10) // 10MB limit
		val := strings.Repeat("test data that should compress well ", 1000)
		for i := 0; i < 100; i++ {
			m1.Set(fmt.Sprint(i), val)
		}

		// Save without compression
		uncompressedPath := filepath.Join(tmpDir, "uncompressed.bin")
		if err := m1.SaveToFileWithOptions(uncompressedPath, SaveOptions{Compress: false}); err != nil {
			t.Fatalf("Failed to save uncompressed: %v", err)
		}

		// Save with compression
		compressedPath := filepath.Join(tmpDir, "compressed.bin")
		if err := m1.SaveToFileWithOptions(compressedPath, SaveOptions{
			Compress:      true,
			CompressLevel: gzip.BestCompression,
		}); err != nil {
			t.Fatalf("Failed to save compressed: %v", err)
		}

		// Compare file sizes
		uncompressedInfo, err := os.Stat(uncompressedPath)
		if err != nil {
			t.Fatal(err)
		}
		compressedInfo, err := os.Stat(compressedPath)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("Uncompressed size: %d bytes", uncompressedInfo.Size())
		t.Logf("Compressed size: %d bytes", compressedInfo.Size())
		t.Logf("Compression ratio: %.2f%%", float64(compressedInfo.Size())/float64(uncompressedInfo.Size())*100)

		if compressedInfo.Size() >= uncompressedInfo.Size() {
			t.Error("Compressed file is not smaller than uncompressed")
		}

		// Load compressed data
		m2 := New[string, string]()
		if err := m2.LoadFromFile(compressedPath); err != nil {
			t.Fatalf("Failed to load compressed data: %v", err)
		}

		// Verify contents
		if m2.Len() != m1.Len() {
			t.Errorf("Loaded map has wrong length: got %d, want %d", m2.Len(), m1.Len())
		}

		v1, _ := m1.Get("0")
		v2, _ := m2.Get("0")
		if v1 != v2 {
			t.Error("Values don't match after compression")
		}
	})

	t.Run("CompressionLevels", func(t *testing.T) {
		m := New[string, string](10)
		val := strings.Repeat("test data that should compress well ", 1000)
		for i := 0; i < 100; i++ {
			m.Set(fmt.Sprint(i), val)
		}

		sizes := make(map[int]int64)
		for level := gzip.BestSpeed; level <= gzip.BestCompression; level++ {
			path := filepath.Join(tmpDir, fmt.Sprintf("level_%d.bin", level))
			if err := m.SaveToFileWithOptions(path, SaveOptions{
				Compress:      true,
				CompressLevel: level,
			}); err != nil {
				t.Fatalf("Failed to save at level %d: %v", level, err)
			}

			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			sizes[level] = info.Size()
			t.Logf("Level %d size: %d bytes", level, info.Size())
		}

		// Verify that higher compression levels generally produce smaller files
		if sizes[gzip.BestCompression] >= sizes[gzip.BestSpeed] {
			t.Error("Best compression did not produce smaller file than best speed")
		}
	})

	t.Run("AsyncCompression", func(t *testing.T) {
		m1 := New[string, string](10)
		val := strings.Repeat("test data that should compress well ", 1000)
		for i := 0; i < 100; i++ {
			m1.Set(fmt.Sprint(i), val)
		}

		// Save with compression asynchronously
		path := filepath.Join(tmpDir, "async_compressed.bin")
		result := m1.SaveToFileAsyncWithOptions(path, SaveOptions{
			Compress:      true,
			CompressLevel: gzip.BestCompression,
		})

		// Monitor progress
		var lastProgress int64
		for {
			select {
			case <-result.Done:
				goto SaveDone
			default:
				progress := result.Progress.Load()
				if progress > lastProgress {
					t.Logf("Save progress: %d%%", progress)
					lastProgress = progress
				}
			}
		}
	SaveDone:
		if result.Error != nil {
			t.Fatalf("Failed to save compressed async: %v", result.Error)
		}

		// Load compressed data asynchronously
		m2 := New[string, string]()
		loadResult := m2.LoadFromFileAsync(path)

		// Monitor progress
		lastProgress = 0
		for {
			select {
			case <-loadResult.Done:
				goto LoadDone
			default:
				progress := loadResult.Progress.Load()
				if progress > lastProgress {
					t.Logf("Load progress: %d%%", progress)
					lastProgress = progress
				}
			}
		}
	LoadDone:
		if loadResult.Error != nil {
			t.Fatalf("Failed to load compressed async: %v", loadResult.Error)
		}

		// Verify contents
		if m2.Len() != m1.Len() {
			t.Errorf("Loaded map has wrong length: got %d, want %d", m2.Len(), m1.Len())
		}

		v1, _ := m1.Get("0")
		v2, _ := m2.Get("0")
		if v1 != v2 {
			t.Error("Values don't match after async compression")
		}
	})
}
