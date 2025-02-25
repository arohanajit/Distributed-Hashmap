package storage

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStore_BasicOperations(t *testing.T) {
	store := NewStore()

	// Test Put
	data := []byte("value1")
	contentType := "text/plain"
	err := store.Put("key1", data, contentType)
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// Test Get
	gotData, gotContentType, err := store.Get("key1")
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !bytes.Equal(gotData, data) {
		t.Errorf("Get returned wrong value: got %s, want %s", gotData, data)
	}
	if gotContentType != contentType {
		t.Errorf("Get returned wrong content type: got %s, want %s", gotContentType, contentType)
	}

	// Test Delete
	err = store.Delete("key1")
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	// Test Get after Delete
	_, _, err = store.Get("key1")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestStore_EmptyKeyValidation(t *testing.T) {
	store := NewStore()

	// Test Put with empty key
	err := store.Put("", []byte("value"), "text/plain")
	if err != ErrEmptyKey {
		t.Errorf("Expected ErrEmptyKey, got %v", err)
	}

	// Test Get with empty key
	_, _, err = store.Get("")
	if err != ErrEmptyKey {
		t.Errorf("Expected ErrEmptyKey, got %v", err)
	}

	// Test Delete with empty key
	err = store.Delete("")
	if err != ErrEmptyKey {
		t.Errorf("Expected ErrEmptyKey, got %v", err)
	}
}

func TestStore_NilValueValidation(t *testing.T) {
	store := NewStore()

	// Test Put with nil value
	err := store.Put("key", nil, "text/plain")
	if err != ErrNilValue {
		t.Errorf("Expected ErrNilValue, got %v", err)
	}
}

func TestStore_ConcurrentOperations(t *testing.T) {
	store := NewStore()
	const numGoroutines = 100
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // For both readers and writers

	// Concurrent writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := string([]byte{byte(id), byte(j)})
				data := []byte{byte(id), byte(j)}
				contentType := "application/octet-stream"
				err := store.Put(key, data, contentType)
				if err != nil {
					t.Errorf("Concurrent Put failed: %v", err)
				}
			}
		}(i)
	}

	// Concurrent readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := string([]byte{byte(id), byte(j)})
				_, _, _ = store.Get(key) // Errors are expected as keys might not exist yet
			}
		}(i)
	}

	wg.Wait()
}

func TestStore_LargeValues(t *testing.T) {
	store := NewStore()
	key := "large-value-key"
	contentType := "application/octet-stream"

	// Create a large value (1MB)
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Test Put
	err := store.Put(key, largeValue, contentType)
	if err != nil {
		t.Errorf("Put() error = %v", err)
		return
	}

	// Test Get
	gotData, _, err := store.Get(key)
	if err != nil {
		t.Errorf("Get() error = %v", err)
		return
	}

	if !bytes.Equal(gotData, largeValue) {
		t.Error("Retrieved data does not match stored data")
	}
}

func TestStore_ValueIsolation(t *testing.T) {
	store := NewStore()

	// Test that values are isolated by key
	tests := []struct {
		key         string
		data        []byte
		contentType string
	}{
		{
			key:         "key1",
			data:        []byte("value1"),
			contentType: "text/plain",
		},
		{
			key:         "key2",
			data:        []byte("value2"),
			contentType: "application/json",
		},
	}

	// Store values
	for _, tt := range tests {
		err := store.Put(tt.key, tt.data, tt.contentType)
		if err != nil {
			t.Errorf("Put() error = %v", err)
			return
		}
	}

	// Verify each value is isolated
	for _, tt := range tests {
		gotData, _, err := store.Get(tt.key)
		if err != nil {
			t.Errorf("Get(%s) error = %v", tt.key, err)
			continue
		}

		if !bytes.Equal(gotData, tt.data) {
			t.Errorf("Get(%s) = %v, want %v", tt.key, gotData, tt.data)
		}
	}
}

func TestStore_SetAndGet(t *testing.T) {
	store := NewStore()

	tests := []struct {
		name        string
		key         string
		value       []byte
		contentType string
		wantErr     error
	}{
		{
			name:        "Store and retrieve JSON data",
			key:         "json-key",
			value:       []byte(`{"test":"value"}`),
			contentType: "application/json",
			wantErr:     nil,
		},
		{
			name:        "Store and retrieve text data",
			key:         "text-key",
			value:       []byte("Hello, World!"),
			contentType: "text/plain",
			wantErr:     nil,
		},
		{
			name:        "Store and retrieve binary data",
			key:         "binary-key",
			value:       []byte{0x00, 0x01, 0x02, 0x03},
			contentType: "application/octet-stream",
			wantErr:     nil,
		},
		{
			name:        "Empty key",
			key:         "",
			value:       []byte("test"),
			contentType: "text/plain",
			wantErr:     ErrEmptyKey,
		},
		{
			name:        "Nil value",
			key:         "nil-key",
			value:       nil,
			contentType: "text/plain",
			wantErr:     ErrNilValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test Put
			err := store.Put(tt.key, tt.value, tt.contentType)
			if err != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr != nil {
				return
			}

			// Test Get
			gotData, gotContentType, err := store.Get(tt.key)
			if err != nil {
				t.Errorf("Get() error = %v", err)
				return
			}

			if !bytes.Equal(gotData, tt.value) {
				t.Errorf("Get() gotData = %v, want %v", gotData, tt.value)
			}
			if gotContentType != tt.contentType {
				t.Errorf("Get() gotContentType = %v, want %v", gotContentType, tt.contentType)
			}
		})
	}
}

func TestStore_Delete(t *testing.T) {
	store := NewStore()

	// Setup test data
	key := "test-key"
	item := &StorageItem{
		Data:        []byte("test-value"),
		ContentType: "text/plain",
	}

	// Store initial data
	err := store.Put(key, item.Data, item.ContentType)
	if err != nil {
		t.Fatalf("Failed to set up test: %v", err)
	}

	tests := []struct {
		name    string
		key     string
		wantErr error
	}{
		{
			name:    "Delete existing key",
			key:     "test-key",
			wantErr: nil,
		},
		{
			name:    "Delete non-existent key",
			key:     "nonexistent",
			wantErr: ErrKeyNotFound,
		},
		{
			name:    "Delete with empty key",
			key:     "",
			wantErr: ErrEmptyKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Delete(tt.key)
			if err != tt.wantErr {
				t.Errorf("Store.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr == nil {
				// Verify key is actually deleted
				_, _, err := store.Get(tt.key)
				if err != ErrKeyNotFound {
					t.Errorf("Store.Get() after delete error = %v, want %v", err, ErrKeyNotFound)
				}
			}
		})
	}
}

func TestStore_UpdateContentType(t *testing.T) {
	store := NewStore()

	// Test updating content type for the same key
	key := "update-key"
	tests := []struct {
		name        string
		value       []byte
		contentType string
	}{
		{
			name:        "Initial JSON",
			value:       []byte(`{"test":"value"}`),
			contentType: "application/json",
		},
		{
			name:        "Update to Text",
			value:       []byte("Updated content"),
			contentType: "text/plain",
		},
		{
			name:        "Update to Binary",
			value:       []byte{0x00, 0x01, 0x02, 0x03},
			contentType: "application/octet-stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Put(key, tt.value, tt.contentType)
			if err != nil {
				t.Errorf("Store.Set() error = %v", err)
				return
			}

			gotData, gotContentType, err := store.Get(key)
			if err != nil {
				t.Errorf("Store.Get() error = %v", err)
				return
			}

			if !bytes.Equal(gotData, tt.value) {
				t.Errorf("Store.Get() value = %v, want %v", gotData, tt.value)
			}

			if gotContentType != tt.contentType {
				t.Errorf("Store.Get() contentType = %v, want %v", gotContentType, tt.contentType)
			}
		})
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	store := NewStore()
	key := "concurrent-key"
	validTypes := map[string]bool{
		"text/plain":               true,
		"application/json":         true,
		"application/octet-stream": true,
	}

	// Run concurrent operations
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("value-%d", i))
			contentType := "text/plain"
			if i%2 == 0 {
				contentType = "application/json"
			} else if i%3 == 0 {
				contentType = "application/octet-stream"
			}
			store.Put(key, data, contentType)
		}(i)
	}

	wg.Wait()

	// Verify that the last write is preserved
	gotData, gotContentType, err := store.Get("concurrent-key")
	if err != nil {
		t.Errorf("Store.Get() after concurrent writes error = %v", err)
		return
	}

	if len(gotData) == 0 {
		t.Error("Got empty data after concurrent writes")
	}

	if !validTypes[gotContentType] {
		t.Errorf("Unexpected content type after concurrent writes: %v", gotContentType)
	}
}

// BenchmarkStore_Put benchmarks the Put operation
func BenchmarkStore_Put(b *testing.B) {
	store := NewStore()
	item := &StorageItem{
		Data:        []byte("test value"),
		ContentType: "text/plain",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		_ = store.Put(key, item.Data, item.ContentType)
	}
}

// BenchmarkStore_Get benchmarks the Get operation
func BenchmarkStore_Get(b *testing.B) {
	store := NewStore()
	item := &StorageItem{
		Data:        []byte("test value"),
		ContentType: "text/plain",
	}

	// Setup: Insert some data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		_ = store.Put(key, item.Data, item.ContentType)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%1000)
		_, _, _ = store.Get(key)
	}
}

// BenchmarkStore_Delete benchmarks the Delete operation
func BenchmarkStore_Delete(b *testing.B) {
	store := NewStore()
	item := &StorageItem{
		Data:        []byte("test value"),
		ContentType: "text/plain",
	}

	// Setup: Insert data that will be deleted
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		_ = store.Put(key, item.Data, item.ContentType)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		_ = store.Delete(key)
	}
}

// TestStore_MixedConcurrentOperations tests a mix of concurrent operations
func TestStore_MixedConcurrentOperations(t *testing.T) {
	store := NewStore()
	const numGoroutines = 100
	const numOperations = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3) // For readers, writers, and deleters

	// Operation counters
	var (
		putSuccesses    uint64
		getSuccesses    uint64
		deleteSuccesses uint64
		putErrors       uint64
		getErrors       uint64
		deleteErrors    uint64
	)

	// Writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				item := &StorageItem{
					Data:        []byte(fmt.Sprintf("value-%d-%d", id, j)),
					ContentType: "text/plain",
				}
				if err := store.Put(key, item.Data, item.ContentType); err != nil {
					atomic.AddUint64(&putErrors, 1)
				} else {
					atomic.AddUint64(&putSuccesses, 1)
				}
			}
		}(i)
	}

	// Readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id%numGoroutines, j%numOperations)
				if _, _, err := store.Get(key); err != nil && err != ErrKeyNotFound {
					atomic.AddUint64(&getErrors, 1)
				} else if err == nil {
					atomic.AddUint64(&getSuccesses, 1)
				}
			}
		}(i)
	}

	// Deleters
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id%numGoroutines, j%numOperations)
				if err := store.Delete(key); err != nil && err != ErrKeyNotFound {
					atomic.AddUint64(&deleteErrors, 1)
				} else if err == nil {
					atomic.AddUint64(&deleteSuccesses, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Put operations - Successes: %d, Errors: %d", putSuccesses, putErrors)
	t.Logf("Get operations - Successes: %d, Errors: %d", getSuccesses, getErrors)
	t.Logf("Delete operations - Successes: %d, Errors: %d", deleteSuccesses, deleteErrors)

	if putErrors > 0 {
		t.Errorf("Expected no put errors, got %d", putErrors)
	}
	if getErrors > 0 {
		t.Errorf("Expected no get errors (excluding KeyNotFound), got %d", getErrors)
	}
	if deleteErrors > 0 {
		t.Errorf("Expected no delete errors (excluding KeyNotFound), got %d", deleteErrors)
	}
}

// TestStore_StressTest performs a stress test with rapid operations
func TestStore_StressTest(t *testing.T) {
	store := NewStore()
	const numOperations = 10000
	const keySpace = 1000

	// Pre-populate some data
	for i := 0; i < keySpace; i++ {
		key := fmt.Sprintf("key-%d", i)
		item := &StorageItem{
			Data:        []byte(fmt.Sprintf("value-%d", i)),
			ContentType: "text/plain",
		}
		if err := store.Put(key, item.Data, item.ContentType); err != nil {
			t.Fatalf("Failed to populate initial data: %v", err)
		}
	}

	// Run stress test
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(3) // One goroutine each for PUT, GET, and DELETE

	// Rapid PUT operations
	go func() {
		defer wg.Done()
		for i := 0; i < numOperations; i++ {
			key := fmt.Sprintf("key-%d", i%keySpace)
			item := &StorageItem{
				Data:        []byte(fmt.Sprintf("value-%d", i)),
				ContentType: "text/plain",
			}
			_ = store.Put(key, item.Data, item.ContentType)
		}
	}()

	// Rapid GET operations
	go func() {
		defer wg.Done()
		for i := 0; i < numOperations; i++ {
			key := fmt.Sprintf("key-%d", i%keySpace)
			_, _, _ = store.Get(key)
		}
	}()

	// Rapid DELETE operations
	go func() {
		defer wg.Done()
		for i := 0; i < numOperations; i++ {
			key := fmt.Sprintf("key-%d", i%keySpace)
			_ = store.Delete(key)
		}
	}()

	wg.Wait()
	duration := time.Since(start)

	t.Logf("Stress test completed in %v", duration)
	t.Logf("Operations per second: %v", float64(numOperations*3)/duration.Seconds())
}

// TestStore_EdgeCases tests various edge cases
func TestStore_EdgeCases(t *testing.T) {
	store := NewStore()

	tests := []struct {
		name    string
		key     string
		value   []byte
		wantErr error
	}{
		{
			name:    "Very long key",
			key:     strings.Repeat("a", 1024*1024), // 1MB key
			value:   []byte("test"),
			wantErr: nil,
		},
		{
			name:    "Key with special characters",
			key:     "!@#$%^&*()",
			value:   []byte("test"),
			wantErr: nil,
		},
		{
			name:    "Key with spaces",
			key:     "key with spaces",
			value:   []byte("test"),
			wantErr: nil,
		},
		{
			name:    "Key with unicode characters",
			key:     "🔑key🔑",
			value:   []byte("test"),
			wantErr: nil,
		},
		{
			name:    "Zero-length value",
			key:     "zero-length",
			value:   []byte{},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &StorageItem{
				Data:        tt.value,
				ContentType: "text/plain",
			}

			// Test Put
			err := store.Put(tt.key, item.Data, item.ContentType)
			if err != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Test Get
			gotData, gotContentType, err := store.Get(tt.key)
			if err != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if !bytes.Equal(gotData, tt.value) {
					t.Errorf("Get() gotData = %v, want %v", gotData, tt.value)
				}
				if gotContentType != item.ContentType {
					t.Errorf("Get() gotContentType = %v, want %v", gotContentType, item.ContentType)
				}
			}

			// Test Delete
			err = store.Delete(tt.key)
			if err != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestStore_RaceConditions tests specific race condition scenarios
func TestStore_RaceConditions(t *testing.T) {
	store := NewStore()
	key := "race-key"
	done := make(chan bool)
	const numGoroutines = 100

	// Test concurrent read/write on same key
	t.Run("Concurrent read/write same key", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(numGoroutines * 2)

		// Writers
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				item := &StorageItem{
					Data:        []byte(fmt.Sprintf("value-%d", id)),
					ContentType: "text/plain",
				}
				_ = store.Put(key, item.Data, item.ContentType)
			}(i)
		}

		// Readers
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				_, _, _ = store.Get(key)
			}()
		}

		wg.Wait()
	})

	// Test concurrent write/delete on same key
	t.Run("Concurrent write/delete same key", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(numGoroutines * 2)

		// Writers
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				item := &StorageItem{
					Data:        []byte(fmt.Sprintf("value-%d", id)),
					ContentType: "text/plain",
				}
				_ = store.Put(key, item.Data, item.ContentType)
			}(i)
		}

		// Deleters
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				_ = store.Delete(key)
			}()
		}

		wg.Wait()

		// Verify the key is in a consistent state (either exists with a value or doesn't exist)
		_, _, err := store.Get(key)
		if err != nil && err != ErrKeyNotFound {
			t.Errorf("Unexpected error state after concurrent write/delete: %v", err)
		}
	})

	// Test rapid put/delete cycles
	t.Run("Rapid put/delete cycles", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				item := &StorageItem{
					Data:        []byte("test"),
					ContentType: "text/plain",
				}
				_ = store.Put(key, item.Data, item.ContentType)
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_ = store.Delete(key)
			}
		}()

		wg.Wait()
	})

	close(done)
}

// TestStore_ContentTypeEdgeCases tests edge cases related to content types
func TestStore_ContentTypeEdgeCases(t *testing.T) {
	store := NewStore()
	key := "test-key"

	tests := []struct {
		name        string
		data        []byte
		contentType string
		wantErr     bool
	}{
		{
			name:        "Empty content type",
			data:        []byte("test"),
			contentType: "",
			wantErr:     false,
		},
		{
			name:        "Very long content type",
			data:        []byte("test"),
			contentType: strings.Repeat("a", 1000),
			wantErr:     false,
		},
		{
			name:        "Content type with special chars",
			data:        []byte("test"),
			contentType: "application/x-custom+json; charset=utf-8",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Put(key, tt.data, tt.contentType)
			if (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			gotData, gotContentType, err := store.Get(key)
			if err != nil {
				t.Errorf("Get() error = %v", err)
				return
			}

			if !bytes.Equal(gotData, tt.data) {
				t.Errorf("Get() gotData = %v, want %v", gotData, tt.data)
			}
			if gotContentType != tt.contentType {
				t.Errorf("Get() gotContentType = %v, want %v", gotContentType, tt.contentType)
			}
		})
	}
}

func TestStore_ConcurrentReadWrite(t *testing.T) {
	store := NewStore()
	key := "concurrent-rw-key"
	contentType := "text/plain"

	// Initialize the key with a value before starting concurrent operations
	initialData := []byte("initial-value")
	err := store.Put(key, initialData, contentType)
	if err != nil {
		t.Fatalf("Failed to initialize key: %v", err)
	}

	// Start multiple writers
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("value-%d", i))
			err := store.Put(key, data, contentType)
			if err != nil {
				t.Errorf("Put() error = %v", err)
			}
		}(i)
	}

	// Start multiple readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, _, err := store.Get(key)
			if err != nil {
				t.Errorf("Get() error = %v", err)
			}
			if len(data) == 0 {
				t.Error("Got empty data")
			}
		}()
	}

	wg.Wait()
}
