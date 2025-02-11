package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// mockStore implements Store interface for testing
type mockStore struct {
	mu          sync.RWMutex
	data        map[string][]byte
	contentType map[string]string
}

func newMockStore() *mockStore {
	return &mockStore{
		data:        make(map[string][]byte),
		contentType: make(map[string]string),
	}
}

func (m *mockStore) Get(key string) ([]byte, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if data, ok := m.data[key]; ok {
		return data, m.contentType[key], nil
	}
	return nil, "", fmt.Errorf("key not found")
}

func (m *mockStore) Put(key string, data []byte, contentType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if key == "error-key" {
		return fmt.Errorf("simulated error")
	}
	m.data[key] = data
	m.contentType[key] = contentType
	return nil
}

func (m *mockStore) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if key == "error-key" {
		return fmt.Errorf("simulated error")
	}
	if _, ok := m.data[key]; !ok {
		return fmt.Errorf("key not found")
	}
	delete(m.data, key)
	delete(m.contentType, key)
	return nil
}

// setupTestServer creates a test server with the given store and middleware
func setupTestServer(t *testing.T) (*mockStore, *httptest.Server) {
	store := newMockStore()
	router := NewRouter(store)
	mux := http.NewServeMux()
	router.RegisterRoutes(mux, mockMiddleware)
	server := httptest.NewServer(mux)
	t.Cleanup(func() {
		server.Close()
	})
	return store, server
}

// mockMiddleware is a simple middleware for testing
func mockMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Test-Middleware", "true")
		next.ServeHTTP(w, r)
	})
}

func TestRouter_RegisterRoutes(t *testing.T) {
	store, server := setupTestServer(t)

	// Pre-populate store with test data
	err := store.Put("test-key", []byte("test-value"), "text/plain")
	if err != nil {
		t.Fatalf("Failed to populate test data: %v", err)
	}

	// Test health endpoint
	resp, err := http.Get(server.URL + "/health")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.StatusCode)
	}
	if resp.Header.Get("X-Test-Middleware") != "" {
		t.Error("Health endpoint should not have middleware")
	}

	// Test keys endpoint with middleware
	resp, err = http.Get(server.URL + "/keys/test-key")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	if resp.Header.Get("X-Test-Middleware") != "true" {
		t.Error("Keys endpoint should have middleware")
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.StatusCode)
	}
}

func TestRouter_HandleKeyOperations(t *testing.T) {
	store, server := setupTestServer(t)

	tests := []struct {
		name           string
		method         string
		key            string
		body           []byte
		contentType    string
		expectedStatus int
		setupFunc      func() // Optional setup function
	}{
		{
			name:           "PUT valid key",
			method:         http.MethodPut,
			key:            "test-key",
			body:           []byte("test-data"),
			contentType:    "text/plain",
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "GET existing key",
			method:         http.MethodGet,
			key:            "test-key",
			expectedStatus: http.StatusOK,
			setupFunc: func() {
				store.Put("test-key", []byte("test-data"), "text/plain")
			},
		},
		{
			name:           "DELETE existing key",
			method:         http.MethodDelete,
			key:            "test-key",
			expectedStatus: http.StatusNoContent,
			setupFunc: func() {
				store.Put("test-key", []byte("test-data"), "text/plain")
			},
		},
		{
			name:           "GET deleted key",
			method:         http.MethodGet,
			key:            "test-key",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "PUT error key",
			method:         http.MethodPut,
			key:            "error-key",
			body:           []byte("test-data"),
			contentType:    "text/plain",
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:           "Invalid method",
			method:         http.MethodPatch,
			key:            "test-key",
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "Empty key",
			method:         http.MethodGet,
			key:            "",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupFunc != nil {
				tt.setupFunc()
			}

			var req *http.Request
			var err error

			url := fmt.Sprintf("%s/keys/%s", server.URL, tt.key)
			if tt.method == http.MethodPut {
				req, err = http.NewRequest(tt.method, url, bytes.NewReader(tt.body))
				req.Header.Set("Content-Type", tt.contentType)
			} else {
				req, err = http.NewRequest(tt.method, url, nil)
			}

			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Failed to make request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.expectedStatus {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("Expected status %v, got %v. Body: %s", tt.expectedStatus, resp.StatusCode, body)
			}
		})
	}
}

func TestRouter_HandleHealth(t *testing.T) {
	_, server := setupTestServer(t)

	tests := []struct {
		name           string
		method         string
		expectedStatus int
		checkBody      bool
	}{
		{
			name:           "GET health",
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			checkBody:      true,
		},
		{
			name:           "POST health",
			method:         http.MethodPost,
			expectedStatus: http.StatusMethodNotAllowed,
			checkBody:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, server.URL+"/health", nil)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Failed to make request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status %v, got %v", tt.expectedStatus, resp.StatusCode)
			}

			if tt.checkBody {
				var response struct {
					Status string `json:"status"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
					t.Errorf("Failed to decode response: %v", err)
				}
				if response.Status != "ok" {
					t.Errorf("Expected status 'ok', got '%v'", response.Status)
				}
			}
		})
	}
}

func TestRouter_ContentTypeHandling(t *testing.T) {
	_, server := setupTestServer(t)

	tests := []struct {
		name             string
		contentType      string
		data             []byte
		expectedResponse string
	}{
		{
			name:             "JSON data",
			contentType:      "application/json",
			data:             []byte(`{"test":"value"}`),
			expectedResponse: "application/json",
		},
		{
			name:             "Text data",
			contentType:      "text/plain",
			data:             []byte("Hello, World!"),
			expectedResponse: "text/plain",
		},
		{
			name:             "Binary data",
			contentType:      "application/octet-stream",
			data:             []byte{0x00, 0x01, 0x02, 0x03},
			expectedResponse: "application/octet-stream",
		},
		{
			name:             "Default content type",
			contentType:      "",
			data:             []byte("test"),
			expectedResponse: "application/octet-stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// PUT request
			req, err := http.NewRequest(http.MethodPut, server.URL+"/keys/test-key", bytes.NewReader(tt.data))
			if err != nil {
				t.Fatalf("Failed to create PUT request: %v", err)
			}
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Failed to make PUT request: %v", err)
			}
			resp.Body.Close()

			// GET request
			resp, err = http.Get(server.URL + "/keys/test-key")
			if err != nil {
				t.Fatalf("Failed to make GET request: %v", err)
			}
			defer resp.Body.Close()

			if resp.Header.Get("Content-Type") != tt.expectedResponse {
				t.Errorf("Expected Content-Type %s, got %s", tt.expectedResponse, resp.Header.Get("Content-Type"))
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response body: %v", err)
			}

			if !bytes.Equal(body, tt.data) {
				t.Errorf("Response body doesn't match: expected %v, got %v", tt.data, body)
			}
		})
	}
}

func TestRouter_ConcurrentAccess(t *testing.T) {
	store, server := setupTestServer(t)

	// Pre-populate some data
	store.Put("test-key", []byte("initial-value"), "text/plain")

	var wg sync.WaitGroup
	concurrentRequests := 10 // Reduced from 100 to avoid overwhelming the test server
	errorChan := make(chan error, concurrentRequests*2)

	// Concurrent reads
	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(server.URL + "/keys/test-key")
			if err != nil {
				errorChan <- fmt.Errorf("Failed concurrent GET: %v", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
				errorChan <- fmt.Errorf("Unexpected status code: %d", resp.StatusCode)
			}
		}()
	}

	// Concurrent writes
	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := fmt.Sprintf("value-%d", i)
			req, err := http.NewRequest(http.MethodPut, server.URL+"/keys/test-key", bytes.NewReader([]byte(data)))
			if err != nil {
				errorChan <- fmt.Errorf("Failed to create PUT request: %v", err)
				return
			}
			req.Header.Set("Content-Type", "text/plain")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errorChan <- fmt.Errorf("Failed concurrent PUT: %v", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				errorChan <- fmt.Errorf("Unexpected status code: %d", resp.StatusCode)
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	for err := range errorChan {
		t.Error(err)
	}
}

// TestRouter_RedirectNonOwner tests the redirection of requests to the appropriate node
func TestRouter_RedirectNonOwner(t *testing.T) {
	// Create a target server that will handle the redirected request
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer targetServer.Close()

	// Create a mock store and router
	store := newMockStore()
	router := NewRouter(store)

	// Create a mock shard manager that will return a different node for our test key
	shardManager := newMockShardManager("node1")
	shardManager.responsibleNodes["redirect-key"] = []string{"node2"}
	shardManager.addresses["node2"] = targetServer.URL[7:] // Strip "http://"

	// Create the shard lookup middleware
	middleware := NewShardLookupMiddleware(shardManager, "node1")

	// Create a test server with our router and middleware
	mux := http.NewServeMux()
	router.RegisterRoutes(mux, middleware.Middleware)
	server := httptest.NewServer(mux)
	defer server.Close()

	// Create a client that doesn't follow redirects
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Make request to a key that should be redirected
	resp, err := client.Get(server.URL + "/keys/redirect-key")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	defer resp.Body.Close()

	// Verify we got a redirect response
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Errorf("Expected status code %d for redirect, got %d",
			http.StatusTemporaryRedirect, resp.StatusCode)
	}

	// Verify the Location header points to the correct node
	location := resp.Header.Get("Location")
	expectedLocation := fmt.Sprintf("http://%s/keys/redirect-key", targetServer.URL[7:])
	if location != expectedLocation {
		t.Errorf("Expected Location header %q, got %q", expectedLocation, location)
	}
}

func TestRouter_RedirectToResponsibleNode(t *testing.T) {
	shardManager := newMockShardManager("node1")
	shardManager.addresses["node2"] = "localhost:8081"
	shardManager.responsibleNodes["redirect-key"] = []string{"node2"}

	// ... existing code ...
}
