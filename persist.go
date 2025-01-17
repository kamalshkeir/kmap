package kmap

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

const (
	magicNumber = uint32(0x4B4D4150) // "KMAP" in ASCII
	version     = uint32(1)
)

// SaveOptions configures how the map is saved to disk
type SaveOptions struct {
	// Compress enables gzip compression of the saved data
	Compress bool
	// CompressLevel sets the gzip compression level (1-9, higher = better compression but slower)
	// Only used if Compress is true. Defaults to gzip.DefaultCompression
	CompressLevel int
}

// SaveResult represents the result of an asynchronous save operation
type SaveResult struct {
	Done     chan struct{}
	Error    error
	Progress atomic.Int64
}

// LoadResult represents the result of an asynchronous load operation
type LoadResult struct {
	Done     chan struct{}
	Error    error
	Progress atomic.Int64
}

// Add at package level
type valueWrapper struct {
	Type  string
	Value []byte
}

// Add at package level
type mapData struct {
	Size  int
	Limit int
	Items map[string]itemData
}

type itemData struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
	Size  int             `json:"size"`
}

// writeBinary writes a value to the writer in a compact binary format
func writeBinary(w io.Writer, v interface{}) error {
	switch val := v.(type) {
	case int:
		return binary.Write(w, binary.LittleEndian, int64(val))
	case int64:
		return binary.Write(w, binary.LittleEndian, val)
	case uint32:
		return binary.Write(w, binary.LittleEndian, val)
	case string:
		length := int32(len(val))
		if err := binary.Write(w, binary.LittleEndian, length); err != nil {
			return err
		}
		_, err := w.Write([]byte(val))
		return err
	default:
		// Create wrapper with type info
		wrapper := valueWrapper{
			Type: fmt.Sprintf("%T", val),
		}

		// Marshal the actual value
		data, err := json.Marshal(val)
		if err != nil {
			return err
		}
		wrapper.Value = data

		// Marshal the wrapper
		wrapperData, err := json.Marshal(wrapper)
		if err != nil {
			return err
		}

		// Write length and data
		length := int32(len(wrapperData))
		if err := binary.Write(w, binary.LittleEndian, length); err != nil {
			return err
		}
		_, err = w.Write(wrapperData)
		return err
	}
}

// readBinary reads a value from the reader in a compact binary format
func readBinary(r io.Reader, into interface{}) error {
	switch val := into.(type) {
	case *int:
		var n int64
		if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
			return err
		}
		*val = int(n)
		return nil
	case *int64:
		return binary.Read(r, binary.LittleEndian, val)
	case *uint32:
		return binary.Read(r, binary.LittleEndian, val)
	case *string:
		var length int32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return err
		}
		if length < 0 || length > 1<<30 {
			return errors.New("invalid string length")
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		*val = string(buf)
		return nil
	default:
		var length int32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return err
		}
		if length < 0 || length > 1<<30 {
			return errors.New("invalid data length")
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}

		// Unmarshal wrapper
		var wrapper valueWrapper
		if err := json.Unmarshal(buf, &wrapper); err != nil {
			return err
		}

		// For interface{} destination
		if v, ok := into.(*interface{}); ok {
			return json.Unmarshal(wrapper.Value, v)
		}
		return json.Unmarshal(wrapper.Value, into)
	}
}

// writeHeader writes the file header
func writeHeader(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, magicNumber); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, version)
}

// readHeader reads and verifies the file header
func readHeader(r io.Reader) error {
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return err
	}
	if magic != magicNumber {
		return errors.New("invalid file format")
	}

	var ver uint32
	if err := binary.Read(r, binary.LittleEndian, &ver); err != nil {
		return err
	}
	if ver != version {
		return errors.New("unsupported file version")
	}

	return nil
}

// SaveToFile saves the SafeMap to a file at the specified path
func (m *SafeMap[K, V]) SaveToFile(path string) error {
	return m.SaveToFileWithOptions(path, SaveOptions{})
}

// SaveToFileWithOptions saves the SafeMap to a file with the specified options
func (m *SafeMap[K, V]) SaveToFileWithOptions(path string, opts SaveOptions) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	var buf bytes.Buffer
	var finalWriter io.Writer = &buf

	if opts.Compress {
		level := opts.CompressLevel
		if level == 0 {
			level = gzip.DefaultCompression
		}
		gzipWriter, err := gzip.NewWriterLevel(&buf, level)
		if err != nil {
			return err
		}
		finalWriter = gzipWriter
		defer gzipWriter.Close()
	}

	m.RLock()
	defer m.RUnlock()

	// Convert items to serializable format
	data := mapData{
		Size:  m.size,
		Limit: m.limit,
		Items: make(map[string]itemData),
	}

	for k, v := range m.items {
		valueBytes, err := json.Marshal(v.Value)
		if err != nil {
			return err
		}

		data.Items[fmt.Sprintf("%v", k)] = itemData{
			Type:  fmt.Sprintf("%T", v.Value),
			Value: valueBytes,
			Size:  v.Size,
		}
	}

	// Write data
	if err := json.NewEncoder(finalWriter).Encode(data); err != nil {
		return err
	}

	return os.WriteFile(path, buf.Bytes(), 0644)
}

// SaveToFileAsync saves the SafeMap to a file asynchronously
func (m *SafeMap[K, V]) SaveToFileAsync(path string) *SaveResult {
	return m.SaveToFileAsyncWithOptions(path, SaveOptions{})
}

// SaveToFileAsyncWithOptions saves the SafeMap to a file asynchronously with the specified options
func (m *SafeMap[K, V]) SaveToFileAsyncWithOptions(path string, opts SaveOptions) *SaveResult {
	result := &SaveResult{
		Done: make(chan struct{}),
	}

	go func() {
		defer close(result.Done)
		result.Error = m.SaveToFileWithOptions(path, opts)
		result.Progress.Store(100)
	}()

	return result
}

// LoadFromFile loads the SafeMap from a file at the specified path
func (m *SafeMap[K, V]) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	reader := bytes.NewReader(data)
	var finalReader io.Reader = reader

	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, gzipReader); err != nil {
			gzipReader.Close()
			return err
		}
		gzipReader.Close()
		finalReader = bytes.NewReader(buf.Bytes())
	}

	m.Lock()
	defer m.Unlock()

	var mapData mapData
	if err := json.NewDecoder(finalReader).Decode(&mapData); err != nil {
		return err
	}

	m.size = mapData.Size
	m.limit = mapData.Limit
	m.items = make(map[K]item[V])

	for kStr, itemData := range mapData.Items {
		var k K
		if err := json.Unmarshal([]byte(fmt.Sprintf("%q", kStr)), &k); err != nil {
			return err
		}

		var v V
		if err := json.Unmarshal(itemData.Value, &v); err != nil {
			return err
		}

		m.items[k] = item[V]{
			Value: v,
			Size:  itemData.Size,
		}
	}

	return nil
}

// LoadFromFileAsync loads the SafeMap from a file asynchronously
func (m *SafeMap[K, V]) LoadFromFileAsync(path string) *LoadResult {
	result := &LoadResult{
		Done: make(chan struct{}),
	}

	go func() {
		defer close(result.Done)
		result.Error = m.LoadFromFile(path)
		result.Progress.Store(100)
	}()

	return result
}

// SaveToFile saves the OrderedMap to a file at the specified path
func (m *OrderedMap[K, V]) SaveToFile(path string) error {
	return m.SaveToFileWithOptions(path, SaveOptions{})
}

// SaveToFileWithOptions saves the OrderedMap to a file with the specified options
func (m *OrderedMap[K, V]) SaveToFileWithOptions(path string, opts SaveOptions) error {
	// Create parent directories if they don't exist
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	// Create a temporary buffer to write data
	var buf bytes.Buffer
	var finalWriter io.Writer = &buf

	var gzipWriter *gzip.Writer
	if opts.Compress {
		level := opts.CompressLevel
		if level == 0 {
			level = gzip.DefaultCompression
		}
		var err error
		gzipWriter, err = gzip.NewWriterLevel(&buf, level)
		if err != nil {
			return err
		}
		finalWriter = gzipWriter
	}

	// Write header
	if err := writeHeader(finalWriter); err != nil {
		return err
	}

	m.RLock()
	defer m.RUnlock()

	// Write map header
	if err := writeBinary(finalWriter, m.size); err != nil {
		return err
	}
	if err := writeBinary(finalWriter, m.limit); err != nil {
		return err
	}
	if err := writeBinary(finalWriter, int64(m.Len())); err != nil {
		return err
	}

	// Write items in order
	for el := m.Front(); el != nil; el = el.Next() {
		if err := writeBinary(finalWriter, el.Key); err != nil {
			return err
		}
		if err := writeBinary(finalWriter, el.Value); err != nil {
			return err
		}
		if err := writeBinary(finalWriter, el.size); err != nil {
			return err
		}
	}

	// Close gzip writer if used
	if gzipWriter != nil {
		if err := gzipWriter.Close(); err != nil {
			return err
		}
	}

	// Write buffer to file
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(buf.Bytes())
	return err
}

// LoadFromFile loads the OrderedMap from a file at the specified path
func (m *OrderedMap[K, V]) LoadFromFile(path string) error {
	// Read entire file into memory
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	// Create reader from data
	reader := bytes.NewReader(data)
	var finalReader io.Reader = reader

	// Check if data is gzip compressed
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return err
		}
		// For gzip reader, we need to read all data into a buffer
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, gzipReader); err != nil {
			gzipReader.Close()
			return err
		}
		if err := gzipReader.Close(); err != nil {
			return err
		}
		finalReader = bytes.NewReader(buf.Bytes())
	}

	// Read and verify header
	if err := readHeader(finalReader); err != nil {
		return err
	}

	// Read map header
	var size, limit int
	var count int64
	if err := readBinary(finalReader, &size); err != nil {
		return err
	}
	if err := readBinary(finalReader, &limit); err != nil {
		return err
	}
	if err := readBinary(finalReader, &count); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	// Clear existing data
	m.kv = make(map[K]*Element[K, V], int(count))
	m.ll = list[K, V]{}
	m.size = size
	m.limit = limit

	// Read items in order
	for i := int64(0); i < count; i++ {
		var k K
		var v V
		var sz int
		if err := readBinary(finalReader, &k); err != nil {
			return err
		}
		if err := readBinary(finalReader, &v); err != nil {
			return err
		}
		if err := readBinary(finalReader, &sz); err != nil {
			return err
		}
		el := m.ll.PushBack(k, v)
		el.size = sz
		m.kv[k] = el
	}

	return nil
}

// SaveToFileAsync saves the OrderedMap to a file asynchronously
func (m *OrderedMap[K, V]) SaveToFileAsync(path string) *SaveResult {
	return m.SaveToFileAsyncWithOptions(path, SaveOptions{})
}

// SaveToFileAsyncWithOptions saves the OrderedMap to a file asynchronously with the specified options
func (m *OrderedMap[K, V]) SaveToFileAsyncWithOptions(path string, opts SaveOptions) *SaveResult {
	result := &SaveResult{
		Done: make(chan struct{}),
	}

	go func() {
		defer close(result.Done)
		result.Error = m.SaveToFileWithOptions(path, opts)
		result.Progress.Store(100)
	}()

	return result
}

// LoadFromFileAsync loads the OrderedMap from a file asynchronously
func (m *OrderedMap[K, V]) LoadFromFileAsync(path string) *LoadResult {
	result := &LoadResult{
		Done: make(chan struct{}),
	}

	go func() {
		defer close(result.Done)
		result.Error = m.LoadFromFile(path)
		result.Progress.Store(100)
	}()

	return result
}
