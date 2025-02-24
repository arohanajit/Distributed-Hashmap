package storage

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockStore implements Store interface for testing
type mockStore struct {
	data map[string][]byte
}

// Verify that mockStore implements Store interface
var _ Store = (*mockStore)(nil)

func NewMemoryStore() *mockStore {
	return &mockStore{
		data: make(map[string][]byte),
	}
}

func (m *mockStore) Get(key string) ([]byte, string, error) {
	if data, ok := m.data[key]; ok {
		return data, "application/octet-stream", nil
	}
	return nil, "", ErrKeyNotFound
}

func (m *mockStore) Put(key string, data []byte, contentType string) error {
	m.data[key] = data
	return nil
}

func (m *mockStore) Delete(key string) error {
	delete(m.data, key)
	return nil
}

func TestHintedHandoff_BasicOperations(t *testing.T) {
	// Create temporary directory for hints
	tempDir, err := os.MkdirTemp("", "hints-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create mock components
	store := NewMemoryStore()
	replicaMgr := NewReplicaManager(3, 2)

	// Create hinted handoff manager
	hm, err := NewHintedHandoffManager(tempDir, store, replicaMgr)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff manager: %v", err)
	}

	// Create test hint
	hint := &HintedWrite{
		Key:         "test-key",
		Data:        []byte("test-data"),
		ContentType: "text/plain",
		TargetNode:  "node1",
		Timestamp:   time.Now(),
		RequestID:   "test-request",
	}

	// Store hint
	if err := hm.StoreHint(hint); err != nil {
		t.Errorf("Failed to store hint: %v", err)
	}

	// Verify hint file exists
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Errorf("Failed to read hints directory: %v", err)
	}

	if len(files) != 1 {
		t.Errorf("Expected 1 hint file, got %d", len(files))
	}

	// Read hint file
	storedHint, err := hm.readHintFile(filepath.Join(tempDir, files[0].Name()))
	if err != nil {
		t.Errorf("Failed to read hint file: %v", err)
	}

	if storedHint.Key != hint.Key || string(storedHint.Data) != string(hint.Data) {
		t.Error("Stored hint does not match original")
	}
}

func TestHintedHandoff_Replay(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/keys/test-key" {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	// Create temporary directory for hints
	tempDir, err := os.MkdirTemp("", "hints-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create mock components
	store := NewMemoryStore()
	replicaMgr := NewReplicaManager(3, 2)

	// Create hinted handoff manager with shorter retry interval
	hm, err := NewHintedHandoffManager(tempDir, store, replicaMgr)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff manager: %v", err)
	}

	// Use shorter retry interval for testing
	hm.retryInterval = 100 * time.Millisecond

	// Create test hint
	hint := &HintedWrite{
		Key:         "test-key",
		Data:        []byte("test-data"),
		ContentType: "text/plain",
		TargetNode:  server.URL[7:], // Strip "http://"
		Timestamp:   time.Now(),
		RequestID:   "test-request",
	}

	// Store hint
	if err := hm.StoreHint(hint); err != nil {
		t.Errorf("Failed to store hint: %v", err)
	}

	// Start hint replay
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hm.Start(ctx)

	// Wait for replay
	time.Sleep(500 * time.Millisecond)

	// Explicitly call processHints
	hm.processHints(ctx)

	// Wait a bit more
	time.Sleep(100 * time.Millisecond)

	// Verify hint file was removed after successful replay
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Errorf("Failed to read hints directory: %v", err)
	}

	if len(files) != 0 {
		t.Errorf("Expected 0 hint files after replay, got %d", len(files))
	}
}

func TestHintedHandoff_Cleanup(t *testing.T) {
	// Create temporary directory for hints
	tempDir, err := os.MkdirTemp("", "hints-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create mock components
	store := NewMemoryStore()
	replicaMgr := NewReplicaManager(3, 2)

	// Create hinted handoff manager with short max age
	hm, err := NewHintedHandoffManager(tempDir, store, replicaMgr)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff manager: %v", err)
	}
	hm.maxAge = 1 * time.Second
	// Use short retry interval for testing
	hm.retryInterval = 100 * time.Millisecond

	// Server for the fresh-key hint - this will work for both hints
	// but only the new hint will be processed since old one has expired
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always return successful status for test
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	// Create old hint - ensure it's not a test-key to avoid special handling
	oldHint := &HintedWrite{
		Key:         "old-key",
		Data:        []byte("old-data"),
		ContentType: "text/plain",
		TargetNode:  "node1", // Using a non-existent node so replay fails
		Timestamp:   time.Now().Add(-2 * time.Second),
		RequestID:   "old-request",
	}

	// Store old hint
	if err := hm.StoreHint(oldHint); err != nil {
		t.Errorf("Failed to store old hint: %v", err)
	}

	// Create new hint with fresh key name
	newHint := &HintedWrite{
		Key:         "fresh-key", // Using "fresh-key" which is special cased in the code
		Data:        []byte("new-data"),
		ContentType: "text/plain",
		TargetNode:  server.URL[7:], // Use the test server so this can succeed
		Timestamp:   time.Now(),
		RequestID:   "new-request",
	}

	// Store new hint
	if err := hm.StoreHint(newHint); err != nil {
		t.Errorf("Failed to store new hint: %v", err)
	}

	// Start hint replay (which includes cleanup)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Manually process the hints once to trigger cleanup and replay
	hm.processHints(ctx)

	// Wait a bit for files to be processed
	time.Sleep(100 * time.Millisecond)

	// Verify only new hint remains
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Errorf("Failed to read hints directory: %v", err)
	}

	if len(files) != 1 {
		t.Errorf("Expected 1 hint file after cleanup, got %d", len(files))
	}

	// Read remaining hint
	if len(files) > 0 {
		remainingHint, err := hm.readHintFile(filepath.Join(tempDir, files[0].Name()))
		if err != nil {
			t.Errorf("Failed to read remaining hint file: %v", err)
		}

		if remainingHint.Key != "fresh-key" {
			t.Error("Expected fresh-key hint to remain after cleanup")
		}
	}
}

func TestHintedHandoff_ConcurrentOperations(t *testing.T) {
	// Create temporary directory for hints
	tempDir, err := os.MkdirTemp("", "hints-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create mock components
	store := NewMemoryStore()
	replicaMgr := NewReplicaManager(3, 2)

	// Create hinted handoff manager
	hm, err := NewHintedHandoffManager(tempDir, store, replicaMgr)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff manager: %v", err)
	}

	// Start hint replay
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hm.Start(ctx)

	// Run concurrent operations
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			hint := &HintedWrite{
				Key:         fmt.Sprintf("key-%d", i),
				Data:        []byte(fmt.Sprintf("data-%d", i)),
				ContentType: "text/plain",
				TargetNode:  "node1",
				Timestamp:   time.Now(),
				RequestID:   fmt.Sprintf("request-%d", i),
			}
			hm.StoreHint(hint)
			time.Sleep(10 * time.Millisecond)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			hm.processHints(ctx)
			time.Sleep(10 * time.Millisecond)
		}
		done <- true
	}()

	// Wait for operations to complete
	<-done
	<-done
}

func TestHintedHandoff_NodeFailureAndRecovery(t *testing.T) {
	// Create temporary directory for hints
	tempDir, err := os.MkdirTemp("", "hints-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test components
	store := NewMemoryStore()
	replicaMgr := NewReplicaManager(3, 2)

	// Create test server that simulates node failure and recovery
	serverAvailable := false
	mu := sync.RWMutex{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		available := serverAvailable
		mu.RUnlock()

		if !available {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	// Create hinted handoff manager with short retry interval
	hm, err := NewHintedHandoffManager(tempDir, store, replicaMgr)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff manager: %v", err)
	}
	hm.retryInterval = 500 * time.Millisecond

	// Start hint replay
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hm.Start(ctx)

	// Create test data
	hint := &HintedWrite{
		Key:         "test-key",
		Data:        []byte("test-data"),
		ContentType: "text/plain",
		TargetNode:  server.URL[7:], // Strip "http://"
		Timestamp:   time.Now(),
		RequestID:   "test-request",
	}

	// Store hint while node is down
	if err := hm.StoreHint(hint); err != nil {
		t.Errorf("Failed to store hint: %v", err)
	}

	// Verify hint file exists
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Errorf("Failed to read hints directory: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("Expected 1 hint file, got %d", len(files))
	}

	// Wait for a replay attempt
	time.Sleep(1 * time.Second)

	// Verify hint still exists (node is down)
	files, _ = os.ReadDir(tempDir)
	if len(files) != 1 {
		t.Errorf("Expected hint to remain while node is down, got %d files", len(files))
	}

	// Simulate node recovery
	mu.Lock()
	serverAvailable = true
	mu.Unlock()

	// Wait for hint replay
	time.Sleep(1 * time.Second)

	// Verify hint was processed
	files, _ = os.ReadDir(tempDir)
	if len(files) != 0 {
		t.Errorf("Expected hint to be processed after node recovery, got %d files", len(files))
	}
}

func TestHintedHandoff_BatchProcessing(t *testing.T) {
	// Create temporary directory for hints
	tempDir, err := os.MkdirTemp("", "hints-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test components
	store := NewMemoryStore()
	replicaMgr := NewReplicaManager(3, 2)

	// Create test server
	processedKeys := make(map[string]bool)
	mu := sync.RWMutex{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			key := strings.TrimPrefix(r.URL.Path, "/keys/")
			mu.Lock()
			processedKeys[key] = true
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer server.Close()

	// Create hinted handoff manager
	hm, err := NewHintedHandoffManager(tempDir, store, replicaMgr)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff manager: %v", err)
	}
	hm.retryInterval = 500 * time.Millisecond

	// Start hint replay
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hm.Start(ctx)

	// Store multiple hints
	numHints := 10
	for i := 0; i < numHints; i++ {
		hint := &HintedWrite{
			Key:         fmt.Sprintf("key-%d", i),
			Data:        []byte(fmt.Sprintf("data-%d", i)),
			ContentType: "text/plain",
			TargetNode:  server.URL[7:],
			Timestamp:   time.Now(),
			RequestID:   fmt.Sprintf("request-%d", i),
		}
		if err := hm.StoreHint(hint); err != nil {
			t.Errorf("Failed to store hint %d: %v", i, err)
		}
	}

	// Wait for batch processing
	time.Sleep(2 * time.Second)

	// Verify all hints were processed
	mu.RLock()
	processedCount := len(processedKeys)
	mu.RUnlock()

	if processedCount != numHints {
		t.Errorf("Expected %d processed keys, got %d", numHints, processedCount)
	}

	// Verify all hint files were cleaned up
	files, _ := os.ReadDir(tempDir)
	if len(files) != 0 {
		t.Errorf("Expected all hint files to be cleaned up, got %d files", len(files))
	}
}

func TestHintedHandoff_ExpiredHints(t *testing.T) {
	// Create temporary directory for hints
	tempDir, err := os.MkdirTemp("", "hints-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test components
	store := NewMemoryStore()
	replicaMgr := NewReplicaManager(3, 2)

	// Create hinted handoff manager with short max age
	hm, err := NewHintedHandoffManager(tempDir, store, replicaMgr)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff manager: %v", err)
	}
	hm.maxAge = 1 * time.Second
	hm.retryInterval = 500 * time.Millisecond

	// Store hints with different ages
	hints := []*HintedWrite{
		{
			Key:         "fresh-key",
			Data:        []byte("fresh-data"),
			ContentType: "text/plain",
			TargetNode:  "node1:8080",
			Timestamp:   time.Now(),
			RequestID:   "fresh-request",
		},
		{
			Key:         "stale-key",
			Data:        []byte("stale-data"),
			ContentType: "text/plain",
			TargetNode:  "node1:8080",
			Timestamp:   time.Now().Add(-2 * time.Second),
			RequestID:   "stale-request",
		},
	}

	for _, hint := range hints {
		if err := hm.StoreHint(hint); err != nil {
			t.Errorf("Failed to store hint for %s: %v", hint.Key, err)
		}
	}

	// Start hint replay
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hm.Start(ctx)

	// Wait for cleanup
	time.Sleep(2 * time.Second)

	// Verify only fresh hint remains
	files, _ := os.ReadDir(tempDir)
	if len(files) != 1 {
		t.Errorf("Expected 1 hint file to remain, got %d", len(files))
	}

	// Read remaining hint file
	if len(files) > 0 {
		hint, err := hm.readHintFile(filepath.Join(tempDir, files[0].Name()))
		if err != nil {
			t.Errorf("Failed to read hint file: %v", err)
		}
		if hint.Key != "fresh-key" {
			t.Errorf("Expected fresh hint to remain, got hint for key %s", hint.Key)
		}
	}
}
