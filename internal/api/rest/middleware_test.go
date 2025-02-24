package rest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// mockShardManager implements ShardManager interface for testing
type mockShardManager struct {
	currentNodeID    string
	addresses        map[string]string // nodeID -> address mapping
	redirectCount    int               // Count redirects for testing loops
	responsibleNodes map[string][]string
}

func newMockShardManager(currentNodeID string) *mockShardManager {
	return &mockShardManager{
		currentNodeID:    currentNodeID,
		addresses:        make(map[string]string),
		responsibleNodes: make(map[string][]string),
	}
}

func (m *mockShardManager) GetNodeForKey(key string) string {
	nodes := m.GetResponsibleNodes(key)
	if len(nodes) > 0 {
		return nodes[0]
	}
	return m.currentNodeID
}

func (m *mockShardManager) GetCurrentNodeID() string {
	return m.currentNodeID
}

// Add GetAllNodes method to mockShardManager
func (m *mockShardManager) GetAllNodes() []string {
	nodes := make([]string, 0)
	for nodeID := range m.addresses {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// Add GetLocalNodeID method to mockShardManager
func (m *mockShardManager) GetLocalNodeID() string {
	return m.currentNodeID
}

// Add GetNodeAddress method to mockShardManager
func (m *mockShardManager) GetNodeAddress(nodeID string) string {
	return m.addresses[nodeID]
}

// Add GetSuccessors method to mockShardManager
func (m *mockShardManager) GetSuccessors(nodeID string) []string {
	return []string{} // Simplified implementation
}

// Add GetPredecessors method to mockShardManager
func (m *mockShardManager) GetPredecessors(nodeID string) []string {
	return []string{} // Simplified implementation
}

// Add GetResponsibleNodes method to mockShardManager
func (m *mockShardManager) GetResponsibleNodes(key string) []string {
	nodes, exists := m.responsibleNodes[key]
	if !exists {
		return []string{m.currentNodeID}
	}
	return nodes
}

// Add GetSuccessorNodes method to mockShardManager
func (m *mockShardManager) GetSuccessorNodes(nodeID string, count int) []string {
	successors := m.GetSuccessors(nodeID)
	if len(successors) > count {
		return successors[:count]
	}
	return successors
}

// Add HasPrimaryShards method to mockShardManager
func (m *mockShardManager) HasPrimaryShards() bool {
	// For testing purposes, return false by default
	return false
}

// HasPrimaryShardsForNode checks if a specific node has primary shards
func (m *mockShardManager) HasPrimaryShardsForNode(nodeID string) bool {
	for _, nodes := range m.responsibleNodes {
		if len(nodes) > 0 && nodes[0] == nodeID {
			return true
		}
	}
	return false
}

// Add UpdateResponsibleNodes method if it's missing
func (m *mockShardManager) UpdateResponsibleNodes(key string, nodes []string) {
	m.responsibleNodes[key] = nodes
}

// mockHandler is a simple handler for testing
type mockHandler struct {
	called bool
}

func (h *mockHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.called = true
	w.WriteHeader(http.StatusOK)
}

func TestShardLookupMiddleware_LocalProcessing(t *testing.T) {
	shardManager := newMockShardManager("node1")
	shardManager.addresses["node1"] = "localhost:8080"

	middleware := NewShardLookupMiddleware(shardManager, "node1")
	handler := &mockHandler{}

	// Create test server
	server := httptest.NewServer(middleware.Middleware(handler))
	defer server.Close()

	// Test request
	resp, err := http.Get(server.URL + "/keys/test-key")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if !handler.called {
		t.Error("Handler should have been called for local node")
	}
}

func TestShardLookupMiddleware_Redirect(t *testing.T) {
	// Create a target server that will handle the proxied request
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/keys/test-key" {
			t.Errorf("Expected path /keys/test-key, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer targetServer.Close()

	shardManager := newMockShardManager("node1")
	shardManager.addresses["node2"] = targetServer.URL[7:] // Strip "http://"
	shardManager.responsibleNodes["test-key"] = []string{"node2"}

	middleware := NewShardLookupMiddleware(shardManager, "node1")
	handler := &mockHandler{}

	// Create test server
	server := httptest.NewServer(middleware.Middleware(handler))
	defer server.Close()

	// Test request
	resp, err := http.Get(server.URL + "/keys/test-key")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status %v, got %v", http.StatusOK, resp.StatusCode)
	}

	if handler.called {
		t.Error("Handler should not have been called for proxied request")
	}
}

func TestShardLookupMiddleware_NoRedirect(t *testing.T) {
	shardManager := newMockShardManager("node1")
	shardManager.responsibleNodes["test-key"] = []string{"node1"}

	middleware := NewShardLookupMiddleware(shardManager, "node1")
	handler := &mockHandler{}

	// Create test server
	server := httptest.NewServer(middleware.Middleware(handler))
	defer server.Close()

	// Test request
	resp, err := http.Get(server.URL + "/keys/test-key")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if !handler.called {
		t.Error("Handler should have been called for local node")
	}
}

func TestShardLookupMiddleware_Error(t *testing.T) {
	shardManager := newMockShardManager("node1")
	shardManager.responsibleNodes["error-key"] = []string{"node2"}
	shardManager.addresses["node2"] = "invalid-host:1234" // Invalid address to force error

	middleware := NewShardLookupMiddleware(shardManager, "node1")
	handler := &mockHandler{}

	// Create test server
	server := httptest.NewServer(middleware.Middleware(handler))
	defer server.Close()

	// Create a client that doesn't follow redirects
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Test request
	resp, err := client.Get(server.URL + "/keys/error-key")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Errorf("Expected status %v, got %v", http.StatusTemporaryRedirect, resp.StatusCode)
	}

	if handler.called {
		t.Error("Handler should not have been called for error")
	}

	// Verify the Location header points to the correct node
	location := resp.Header.Get("Location")
	expectedLocation := "http://invalid-host:1234/keys/error-key"
	if location != expectedLocation {
		t.Errorf("Expected Location header %q, got %q", expectedLocation, location)
	}
}

func TestShardLookupMiddleware_ErrorCases(t *testing.T) {
	shardManager := newMockShardManager("node1")
	// Set up error condition for error-key
	shardManager.responsibleNodes["error-key"] = []string{"node2"}
	shardManager.addresses["node2"] = "invalid-host:1234" // Invalid address to force error

	middleware := NewShardLookupMiddleware(shardManager, "node1")
	handler := &mockHandler{}

	tests := []struct {
		name           string
		path           string
		expectedStatus int
	}{
		{
			name:           "Empty key",
			path:           "/keys/",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Error key",
			path:           "/keys/error-key",
			expectedStatus: http.StatusTemporaryRedirect,
		},
		{
			name:           "Non-keys path",
			path:           "/other/path",
			expectedStatus: http.StatusOK,
		},
	}

	server := httptest.NewServer(middleware.Middleware(handler))
	defer server.Close()

	// Create a client that doesn't follow redirects
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := client.Get(server.URL + tt.path)
			if err != nil {
				t.Fatalf("Failed to make request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status %v, got %v", tt.expectedStatus, resp.StatusCode)
			}

			if tt.name == "Error key" {
				location := resp.Header.Get("Location")
				expectedLocation := "http://invalid-host:1234/keys/error-key"
				if location != expectedLocation {
					t.Errorf("Expected Location header %q, got %q", expectedLocation, location)
				}
			}
		})
	}
}

func TestShardLookupMiddleware_RedirectLoop(t *testing.T) {
	// Create a chain of test servers
	servers := make([]*httptest.Server, 3)
	for i := range servers {
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer servers[i].Close()
	}

	shardManager := newMockShardManager("node1")
	// Set up a chain of responsible nodes
	shardManager.addresses["node2"] = servers[0].URL[7:]
	shardManager.addresses["node3"] = servers[1].URL[7:]
	shardManager.addresses["node4"] = servers[2].URL[7:]
	shardManager.responsibleNodes["loop-key"] = []string{"node2", "node3", "node4"}

	middleware := NewShardLookupMiddleware(shardManager, "node1")
	handler := &mockHandler{}

	// Create test server
	server := httptest.NewServer(middleware.Middleware(handler))
	defer server.Close()

	// Test request
	resp, err := http.Get(server.URL + "/keys/loop-key")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status %v, got %v", http.StatusOK, resp.StatusCode)
	}

	if handler.called {
		t.Error("Handler should not have been called for proxied request")
	}
}

func TestShardLookupMiddleware_ProxyRequest(t *testing.T) {
	// Create a target server
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer targetServer.Close()

	shardManager := newMockShardManager("node1")
	middleware := NewShardLookupMiddleware(shardManager, "node1")

	// Test the proxy function directly
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test", nil)
	r.Header.Set("X-Custom-Header", "test-value")

	middleware.proxyRequest(targetServer.URL, w, r)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.StatusCode)
	}

	if resp.Header.Get("Content-Type") != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", resp.Header.Get("Content-Type"))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	expected := `{"status":"ok"}`
	if string(body) != expected {
		t.Errorf("Expected body %q, got %q", expected, string(body))
	}
}

func TestShardLookupMiddleware_ProxyRequestErrors(t *testing.T) {
	shardManager := newMockShardManager("node1")
	middleware := NewShardLookupMiddleware(shardManager, "node1")

	tests := []struct {
		name           string
		targetURL      string
		expectedStatus int
	}{
		{
			name:           "Invalid URL",
			targetURL:      "://invalid-url",
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:           "Non-existent server",
			targetURL:      "http://non-existent-server:12345",
			expectedStatus: http.StatusBadGateway,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/test", nil)

			middleware.proxyRequest(tt.targetURL, w, r)

			resp := w.Result()
			defer resp.Body.Close()

			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status %v, got %v", tt.expectedStatus, resp.StatusCode)
			}
		})
	}
}
