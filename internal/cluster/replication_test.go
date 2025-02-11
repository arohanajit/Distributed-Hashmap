package cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arohanajit/Distributed-Hashmap/internal/storage"
)

func TestReplicator_BasicReplication(t *testing.T) {
	// Create test servers
	servers := make(map[string]*httptest.Server)
	for _, nodeID := range []string{"node1", "node2", "node3"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				t.Errorf("Expected PUT request, got %s", r.Method)
			}
			if r.Header.Get("X-Request-ID") == "" {
				t.Error("Missing request ID header")
			}
			w.WriteHeader(http.StatusCreated)
		}))
		servers[nodeID] = server
		defer server.Close()
	}

	// Create mock shard manager
	shardMgr := &mockShardManager{
		responsibleNodes: map[string][]string{
			"test-key": {"node1", "node2", "node3"},
		},
		nodeAddresses: make(map[string]string),
	}

	// Set up node addresses
	for nodeID, server := range servers {
		shardMgr.nodeAddresses[nodeID] = server.URL[7:]
	}

	// Create replicator with quorum of 2
	replicator := NewReplicator(3, 2, shardMgr)

	// Test replication
	ctx := context.Background()
	err := replicator.ReplicateKey(ctx, "test-key", []byte("test-data"), "text/plain", "node1")
	if err != nil {
		t.Errorf("Replication failed: %v", err)
	}

	// Verify replica status
	status, err := replicator.GetReplicaStatus("test-key")
	if err != nil {
		t.Errorf("Failed to get replica status: %v", err)
	}

	successCount := 0
	for _, s := range status {
		if s == storage.ReplicaSuccess {
			successCount++
		}
	}

	if successCount < 2 {
		t.Errorf("Expected at least 2 successful replicas, got %d", successCount)
	}
}

func TestReplicator_FailedReplication(t *testing.T) {
	// Create a server that always fails
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failServer.Close()

	// Setup mock shard manager
	shardMgr := newMockShardManager("node1")
	shardMgr.responsibleNodes = map[string][]string{
		"test-key": {"node1", "node2"},
	}
	shardMgr.nodeAddresses = map[string]string{
		"node2": failServer.URL[7:], // Strip "http://"
	}

	// Create replicator with short timeout
	replicator := NewReplicator(2, 2, shardMgr)
	replicator.client.Timeout = 100 * time.Millisecond

	// Test replication
	ctx := context.Background()
	err := replicator.ReplicateKey(ctx, "test-key", []byte("test-data"), "text/plain", "node1")
	if err == nil {
		t.Error("Expected replication to fail, but it succeeded")
	}
}

func TestReplicator_QuorumSuccess(t *testing.T) {
	// Create test servers
	servers := make(map[string]*httptest.Server)
	successfulNodes := make(map[string]bool)

	// Set up mock servers
	for _, nodeID := range []string{"node1", "node2", "node3"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				t.Errorf("Expected PUT request, got %s", r.Method)
			}
			if r.Header.Get("X-Request-ID") == "" {
				t.Error("Missing request ID header")
			}

			// Get the node ID from the server URL
			serverURL := r.Host
			var currentNodeID string
			for nID, srv := range servers {
				if strings.Contains(srv.URL, serverURL) {
					currentNodeID = nID
					break
				}
			}

			if successfulNodes[currentNodeID] {
				w.WriteHeader(http.StatusCreated)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
		}))
		servers[nodeID] = server
		defer server.Close()
	}

	// Create mock shard manager
	shardMgr := &mockShardManager{
		responsibleNodes: map[string][]string{
			"test-key": {"node1", "node2", "node3"},
		},
		nodeAddresses: make(map[string]string),
	}

	// Set up node addresses
	for nodeID, server := range servers {
		shardMgr.nodeAddresses[nodeID] = server.URL[7:] // Strip "http://"
	}

	// Test cases
	tests := []struct {
		name          string
		successNodes  []string
		expectSuccess bool
	}{
		{
			name:          "Quorum met - all nodes succeed",
			successNodes:  []string{"node1", "node2", "node3"},
			expectSuccess: true,
		},
		{
			name:          "Quorum met - two nodes succeed",
			successNodes:  []string{"node1", "node2"},
			expectSuccess: true,
		},
		{
			name:          "Quorum not met - one node succeeds",
			successNodes:  []string{"node1"},
			expectSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset successful nodes
			for nodeID := range successfulNodes {
				successfulNodes[nodeID] = false
			}
			// Set successful nodes for this test
			for _, nodeID := range tt.successNodes {
				successfulNodes[nodeID] = true
			}

			replicator := NewReplicator(3, 2, shardMgr)
			err := replicator.ReplicateKey(context.Background(), "test-key", []byte("test-data"), "text/plain", "node1")

			if tt.expectSuccess && err != nil {
				t.Errorf("Expected success, got error: %v", err)
			} else if !tt.expectSuccess && err == nil {
				t.Error("Expected error, got success")
			}

			// Verify replica status
			status, err := replicator.GetReplicaStatus("test-key")
			if err != nil {
				t.Errorf("Failed to get replica status: %v", err)
				return
			}

			successCount := 0
			for nodeID, s := range status {
				if s == storage.ReplicaSuccess {
					if !successfulNodes[nodeID] {
						t.Errorf("Node %s marked as successful but should have failed", nodeID)
					}
					successCount++
				}
			}

			if tt.expectSuccess && successCount < 2 {
				t.Errorf("Expected at least 2 successful replicas, got %d", successCount)
			}
		})
	}
}

func TestReplicator_Idempotency(t *testing.T) {
	// Create test servers
	servers := make(map[string]*httptest.Server)
	requestCounts := make(map[string]int)
	var mu sync.Mutex

	// Set up mock servers
	for _, nodeID := range []string{"node1", "node2", "node3"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				t.Errorf("Expected PUT request, got %s", r.Method)
			}

			requestID := r.Header.Get("X-Request-ID")
			if requestID == "" {
				t.Error("Missing request ID header")
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			// Track request count for this node
			serverURL := r.Host
			var currentNodeID string
			for nID, srv := range servers {
				if strings.Contains(srv.URL, serverURL) {
					currentNodeID = nID
					break
				}
			}

			mu.Lock()
			requestCounts[currentNodeID]++
			mu.Unlock()

			// Always return success
			w.WriteHeader(http.StatusCreated)
		}))
		servers[nodeID] = server
		defer server.Close()
	}

	// Create mock shard manager
	shardMgr := &mockShardManager{
		responsibleNodes: map[string][]string{
			"test-key": {"node1", "node2", "node3"},
		},
		nodeAddresses: make(map[string]string),
	}

	// Set up node addresses
	for nodeID, server := range servers {
		shardMgr.nodeAddresses[nodeID] = server.URL[7:]
	}

	replicator := NewReplicator(3, 2, shardMgr)

	// Perform multiple replications with the same request ID
	key := "test-key"
	data := []byte("test-data")
	contentType := "text/plain"
	primaryNode := "node1"

	// First replication
	err := replicator.ReplicateKey(context.Background(), key, data, contentType, primaryNode)
	if err != nil {
		t.Errorf("First replication failed: %v", err)
	}

	// Second replication with same data
	err = replicator.ReplicateKey(context.Background(), key, data, contentType, primaryNode)
	if err != nil {
		t.Errorf("Second replication failed: %v", err)
	}

	// Verify request counts
	for nodeID, count := range requestCounts {
		if count > 2 {
			t.Errorf("Node %s received %d requests, expected at most 2", nodeID, count)
		}
	}

	// Verify final replica status
	status, err := replicator.GetReplicaStatus(key)
	if err != nil {
		t.Errorf("Failed to get replica status: %v", err)
		return
	}

	successCount := 0
	for _, s := range status {
		if s == storage.ReplicaSuccess {
			successCount++
		}
	}

	if successCount < 2 {
		t.Errorf("Expected at least 2 successful replicas, got %d", successCount)
	}
}

func TestReplicator_QuorumCompliance(t *testing.T) {
	tests := []struct {
		name          string
		replicaCount  int
		writeQuorum   int
		successNodes  int
		expectSuccess bool
	}{
		{
			name:          "Meet write quorum",
			replicaCount:  3,
			writeQuorum:   2,
			successNodes:  2,
			expectSuccess: true,
		},
		{
			name:          "Fail write quorum",
			replicaCount:  3,
			writeQuorum:   2,
			successNodes:  1,
			expectSuccess: false,
		},
		{
			name:          "All nodes success",
			replicaCount:  3,
			writeQuorum:   2,
			successNodes:  3,
			expectSuccess: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test servers
			servers := make(map[string]*httptest.Server)
			successfulNodes := make(map[string]bool)

			// Set up mock servers
			for i := 0; i < tt.replicaCount; i++ {
				nodeID := fmt.Sprintf("node%d", i+1)
				successfulNodes[nodeID] = i < tt.successNodes

				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPut {
						t.Errorf("Expected PUT request, got %s", r.Method)
					}
					if r.Header.Get("X-Request-ID") == "" {
						t.Error("Missing request ID header")
					}

					// Get the node ID from the server URL
					serverURL := r.Host
					var currentNodeID string
					for nID, srv := range servers {
						if strings.Contains(srv.URL, serverURL) {
							currentNodeID = nID
							break
						}
					}

					if successfulNodes[currentNodeID] {
						w.WriteHeader(http.StatusCreated)
					} else {
						w.WriteHeader(http.StatusInternalServerError)
					}
				}))
				servers[nodeID] = server
				defer server.Close()
			}

			// Create mock shard manager
			shardMgr := &mockShardManager{
				responsibleNodes: map[string][]string{
					"test-key": make([]string, tt.replicaCount),
				},
				nodeAddresses: make(map[string]string),
			}

			// Set up responsible nodes and addresses
			for i := 0; i < tt.replicaCount; i++ {
				nodeID := fmt.Sprintf("node%d", i+1)
				shardMgr.responsibleNodes["test-key"][i] = nodeID
				shardMgr.nodeAddresses[nodeID] = servers[nodeID].URL[7:]
			}

			replicator := NewReplicator(tt.replicaCount, tt.writeQuorum, shardMgr)
			err := replicator.ReplicateKey(context.Background(), "test-key", []byte("test-data"), "text/plain", "node1")

			if tt.expectSuccess && err != nil {
				if !strings.Contains(err.Error(), "some replications failed") {
					t.Errorf("Expected success or partial failure, got error: %v", err)
				}
			} else if !tt.expectSuccess && err == nil {
				t.Error("Expected error, got success")
			}

			// Verify replica status
			status, err := replicator.GetReplicaStatus("test-key")
			if err != nil {
				t.Errorf("Failed to get replica status: %v", err)
				return
			}

			successCount := 0
			for nodeID, s := range status {
				if s == storage.ReplicaSuccess {
					if !successfulNodes[nodeID] {
						t.Errorf("Node %s marked as successful but should have failed", nodeID)
					}
					successCount++
				}
			}

			if tt.expectSuccess && successCount < tt.writeQuorum {
				t.Errorf("Expected at least %d successful replicas, got %d", tt.writeQuorum, successCount)
			}
		})
	}
}

func TestReplicator_ReReplication(t *testing.T) {
	// Create test servers
	servers := make(map[string]*httptest.Server)
	data := make(map[string][]byte)
	mu := sync.RWMutex{}

	for _, nodeID := range []string{"node1", "node2", "node3", "node4"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()

			key := strings.TrimPrefix(r.URL.Path, "/keys/")
			if r.Method == http.MethodPut {
				body, _ := io.ReadAll(r.Body)
				data[key] = body
				w.WriteHeader(http.StatusCreated)
			} else if r.Method == http.MethodGet {
				if val, ok := data[key]; ok {
					w.Write(val)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}
			}
		}))
		servers[nodeID] = server
		defer server.Close()
	}

	// Setup mock shard manager
	shardMgr := newMockShardManager("node1")
	shardMgr.responsibleNodes = map[string][]string{
		"test-key": {"node1", "node2", "node3"},
	}
	for nodeID, server := range servers {
		shardMgr.nodeAddresses[nodeID] = server.URL[7:]
	}

	// Create failure detector
	fd := NewFailureDetector(100*time.Millisecond, 2)
	for nodeID := range servers {
		fd.AddNode(nodeID, shardMgr.nodeAddresses[nodeID])
	}

	// Create store and replica manager
	store := &mockStore{keys: make(map[string][]byte)}
	replicaMgr := storage.NewReplicaManager(3, 2)

	// Create re-replication manager
	rm := NewReReplicationManager(store, replicaMgr, shardMgr, fd)

	// Start re-replication manager
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rm.Start(ctx)

	// Write initial data
	key := "test-key"
	value := []byte("test-data")
	replicator := NewReplicator(3, 2, shardMgr)
	err := replicator.ReplicateKey(ctx, key, value, "text/plain", "node1")
	if err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}

	// Simulate node failure
	fd.nodes["node2"].IsHealthy = false
	fd.nodes["node2"].MissedBeats = 3

	// Wait for re-replication
	time.Sleep(2 * time.Second)

	// Verify data was re-replicated to node4
	mu.RLock()
	_, hasData := data[key]
	mu.RUnlock()
	if !hasData {
		t.Error("Data was not re-replicated to new node")
	}
}
