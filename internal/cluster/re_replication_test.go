package cluster

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/arohanajit/Distributed-Hashmap/internal/storage"
)

// mockStore implements storage.Store interface
type mockStore struct {
	keys map[string][]byte
}

func (m *mockStore) Get(key string) ([]byte, string, error) {
	if data, ok := m.keys[key]; ok {
		return data, "application/octet-stream", nil
	}
	return nil, "", storage.ErrKeyNotFound
}

func (m *mockStore) GetAllKeys() []string {
	keys := make([]string, 0, len(m.keys))
	for k := range m.keys {
		keys = append(keys, k)
	}
	return keys
}

func (m *mockStore) Put(key string, data []byte, contentType string) error {
	m.keys[key] = data
	return nil
}

func (m *mockStore) Delete(key string) error {
	delete(m.keys, key)
	return nil
}

// testShardManager implements cluster.ShardManager interface
type testShardManager struct {
	responsibleNodes map[string][]string
	nodeAddresses    map[string]string
	currentNodeID    string
}

func newTestShardManager() *testShardManager {
	return &testShardManager{
		responsibleNodes: make(map[string][]string),
		nodeAddresses:    make(map[string]string),
	}
}

func (m *testShardManager) GetResponsibleNodes(key string) []string {
	return m.responsibleNodes[key]
}

func (m *testShardManager) GetSuccessorNodes(nodeID string, count int) []string {
	nodes := make([]string, 0, count)
	allNodes := m.GetAllNodes()

	// Find the index of the current node
	currentIdx := -1
	for i, id := range allNodes {
		if id == nodeID {
			currentIdx = i
			break
		}
	}

	// If node not found, start from beginning
	if currentIdx == -1 {
		currentIdx = 0
	}

	// Get the next 'count' nodes after the current node
	for i := 0; i < count; i++ {
		nextIdx := (currentIdx + i + 1) % len(allNodes)
		nodes = append(nodes, allNodes[nextIdx])
	}

	return nodes
}

func (m *testShardManager) GetNodeAddress(nodeID string) string {
	return m.nodeAddresses[nodeID]
}

func (m *testShardManager) GetAllNodes() []string {
	nodes := make([]string, 0, len(m.nodeAddresses))
	for nodeID := range m.nodeAddresses {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

func (m *testShardManager) GetCurrentNodeID() string {
	return m.currentNodeID
}

func (m *testShardManager) GetNodeForKey(key string) string {
	if nodes := m.GetResponsibleNodes(key); len(nodes) > 0 {
		return nodes[0]
	}
	return ""
}

func (m *testShardManager) GetSuccessors(nodeID string) []string {
	return m.GetSuccessorNodes(nodeID, 2) // Default to 2 successors
}

func (m *testShardManager) GetPredecessors(nodeID string) []string {
	// Simple implementation for testing
	return []string{nodeID + "-pred1", nodeID + "-pred2"}
}

// Add HasPrimaryShards method
func (m *testShardManager) HasPrimaryShards() bool {
	return false
}

func (m *testShardManager) HasPrimaryShardsForNode(nodeID string) bool {
	// Check if the given node is the primary node for any key
	for _, nodes := range m.responsibleNodes {
		if len(nodes) > 0 && nodes[0] == nodeID {
			return true
		}
	}
	return false
}

func (m *testShardManager) GetLocalNodeID() string {
	return m.currentNodeID
}

// Add UpdateResponsibleNodes method
func (m *testShardManager) UpdateResponsibleNodes(key string, nodes []string) {
	m.responsibleNodes[key] = nodes
}

func TestReReplicationManager_BasicOperations(t *testing.T) {
	// Create test servers
	servers := make(map[string]*httptest.Server)
	nodeIDs := []string{"node1", "node2", "node3", "node4", "node5"} // Added node5 for re-replication
	for _, nodeID := range nodeIDs {
		nodeID := nodeID // Capture for closure
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Path == "/health" {
				if nodeID == "node2" {
					w.WriteHeader(http.StatusServiceUnavailable) // Make node2 unhealthy
					return
				}
				w.WriteHeader(http.StatusOK)
				return
			}
			if r.Method == http.MethodPut {
				w.WriteHeader(http.StatusCreated) // Simulate successful replication
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
		}))
		servers[nodeID] = server
		defer server.Close()
	}

	// Create test components
	store := &mockStore{
		keys: map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
		},
	}

	replicaMgr := storage.NewReplicaManager(3, 2)
	shardMgr := newTestShardManager()
	shardMgr.currentNodeID = "node1"

	// Use dynamic addresses from test servers:
	for nodeID, server := range servers {
		shardMgr.nodeAddresses[nodeID] = server.URL[7:]
	}

	// Set up node responsibilities
	shardMgr.responsibleNodes = map[string][]string{
		"key1": {"node1", "node2", "node3"},
		"key2": {"node2", "node3", "node4"},
	}

	// Create and start failure detector with shorter intervals for testing
	fd := NewFailureDetector(50*time.Millisecond, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fd.Start(ctx)

	// Add nodes to failure detector
	for nodeID, addr := range shardMgr.nodeAddresses {
		fd.AddNode(nodeID, addr)
	}

	// Create and start re-replication manager with shorter interval
	rm := NewReReplicationManagerWithInterval(store, replicaMgr, shardMgr, fd, 100*time.Millisecond)
	rm.Start(ctx)
	defer rm.Stop()

	// Initialize replication tracking for key1
	replicaMgr.InitReplication("key1", "test-request", []string{"node1", "node2", "node3"})

	// Initialize replication tracking for key2
	replicaMgr.InitReplication("key2", "test-request-2", []string{"node2", "node3", "node4"})

	// Set initial replica states
	replicaMgr.UpdateReplicaStatus("key1", "node1", storage.ReplicaSuccess)
	replicaMgr.UpdateReplicaStatus("key1", "node2", storage.ReplicaSuccess)
	replicaMgr.UpdateReplicaStatus("key1", "node3", storage.ReplicaSuccess)

	replicaMgr.UpdateReplicaStatus("key2", "node2", storage.ReplicaSuccess)
	replicaMgr.UpdateReplicaStatus("key2", "node3", storage.ReplicaSuccess)
	replicaMgr.UpdateReplicaStatus("key2", "node4", storage.ReplicaSuccess)

	// Wait for initial health checks and node2 to be marked as unhealthy
	deadline := time.Now().Add(5 * time.Second)
	var node2Failed bool
	for time.Now().Before(deadline) {
		if !fd.IsNodeHealthy("node2") {
			node2Failed = true
			t.Log("Node2 marked as unhealthy")
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !node2Failed {
		t.Fatal("Node2 was not marked as unhealthy within the timeout period")
	}

	// Wait for re-replication to complete
	success := false
	deadline = time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		// Get current status
		status, err := replicaMgr.GetReplicaStatus("key1")
		if err != nil {
			t.Fatalf("Failed to get replica status: %v", err)
		}

		t.Logf("Current status: %v", status)

		// Check if node2 is marked as failed
		if status["node2"] != storage.ReplicaFailed {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Look for a new replica in pending or success state
		for nodeID, st := range status {
			if nodeID != "node1" && nodeID != "node2" && nodeID != "node3" &&
				(st == storage.ReplicaPending || st == storage.ReplicaSuccess) {
				success = true
				t.Logf("Found new replica on node %s with status %v", nodeID, st)
				break
			}
		}

		if success {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !success {
		status, _ := replicaMgr.GetReplicaStatus("key1")
		t.Errorf("Re-replication failed. Final status: %v", status)
		t.Errorf("Node addresses: %v", shardMgr.nodeAddresses)
		t.Errorf("Responsible nodes: %v", shardMgr.responsibleNodes)
		t.Errorf("Node2 health: %v", fd.IsNodeHealthy("node2"))

		// Print health status of all nodes
		for _, nodeID := range nodeIDs {
			t.Logf("Node %s health: %v", nodeID, fd.IsNodeHealthy(nodeID))
		}
	}
}

func TestReReplicationManager_QuorumHandling(t *testing.T) {
	// Create test server that simulates successful replication
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	// Create test components
	store := &mockStore{
		keys: map[string][]byte{
			"key1": []byte("test-data"),
		},
	}

	replicaMgr := storage.NewReplicaManager(3, 2) // 3 replicas, quorum of 2
	shardMgr := newTestShardManager()

	// Register all test nodes with addresses
	testNodes := []string{"node1", "node2", "node3", "node4"}
	for _, nodeID := range testNodes {
		shardMgr.nodeAddresses[nodeID] = server.URL[7:] // Use test server for all nodes
	}

	// Set up node responsibilities
	shardMgr.responsibleNodes = map[string][]string{
		"key1": {"node1", "node2", "node3"},
	}

	// Create and start failure detector
	fd := NewFailureDetector(100*time.Millisecond, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fd.Start(ctx)

	// Add nodes to failure detector
	for nodeID, addr := range shardMgr.nodeAddresses {
		fd.AddNode(nodeID, addr)
	}

	rm := NewReReplicationManager(store, replicaMgr, shardMgr, fd)

	// Initialize replication tracking with initial state
	requestID := "test-request"
	replicaMgr.InitReplication("key1", requestID, []string{"node1", "node2", "node3"})

	// Set initial replica states
	replicaMgr.UpdateReplicaStatus("key1", "node1", storage.ReplicaSuccess)
	replicaMgr.UpdateReplicaStatus("key1", "node2", storage.ReplicaFailed)
	replicaMgr.UpdateReplicaStatus("key1", "node3", storage.ReplicaSuccess)

	// Mark node2 as unhealthy in failure detector
	fd.nodes["node2"].IsHealthy = false
	fd.nodes["node2"].MissedBeats = fd.threshold + 1

	// Wait for failure detector to process
	time.Sleep(200 * time.Millisecond)

	// Trigger re-replication
	rm.checkAndRebalance(ctx)

	// Wait for re-replication to complete and verify status periodically
	deadline := time.Now().Add(5 * time.Second)
	var quorumMet bool
	var err error

	for time.Now().Before(deadline) {
		quorumMet, err = replicaMgr.CheckQuorum("key1")
		if err != nil {
			t.Fatalf("Failed to check quorum: %v", err)
		}
		if quorumMet {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !quorumMet {
		// Get current replica status for debugging
		status, _ := replicaMgr.GetReplicaStatus("key1")
		t.Errorf("Expected quorum to be met after re-replication. Current replica status: %v", status)
	}
}

func TestReReplicationManager_ConcurrentOperations(t *testing.T) {
	// Create test servers
	servers := make(map[string]*httptest.Server)
	for i := 1; i <= 5; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut {
				w.WriteHeader(http.StatusCreated) // Simulate successful replication
			}
		}))
		defer server.Close()
		servers[nodeID] = server
	}

	// Create mock components
	store := &mockStore{
		keys: map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
			"key3": []byte("value3"),
		},
	}

	replicaMgr := storage.NewReplicaManager(3, 2)
	shardMgr := &testShardManager{
		responsibleNodes: map[string][]string{
			"key1": {"node1", "node2", "node3"},
			"key2": {"node2", "node3", "node4"},
			"key3": {"node3", "node4", "node5"},
		},
		// Use dynamic addresses from test servers:
		nodeAddresses: map[string]string{
			"node1": servers["node1"].URL[7:],
			"node2": servers["node2"].URL[7:],
			"node3": servers["node3"].URL[7:],
			"node4": servers["node4"].URL[7:],
			"node5": servers["node5"].URL[7:],
		},

		currentNodeID: "node1",
	}

	fd := NewFailureDetector(100*time.Millisecond, 2) // Shorter interval for tests
	rm := NewReReplicationManager(store, replicaMgr, shardMgr, fd)

	// Start re-replication manager *and* failure detector
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rm.Start(ctx)
	go fd.Start(ctx) // MUST start the failure detector!

	// Add nodes to the failure detector, using addresses from shard manager
	for nodeID, addr := range shardMgr.nodeAddresses {
		fd.AddNode(nodeID, addr)
	}

	// Initialize replication state for keys
	replicaMgr.InitReplication("key1", "request1", []string{"node1", "node2", "node3"})
	replicaMgr.InitReplication("key2", "request2", []string{"node2", "node3", "node4"})
	replicaMgr.InitReplication("key3", "request3", []string{"node3", "node4", "node5"})

	// Set initial replica states (all successful to start)
	for _, key := range []string{"key1", "key2", "key3"} {
		for _, nodeID := range shardMgr.responsibleNodes[key] {
			replicaMgr.UpdateReplicaStatus(key, nodeID, storage.ReplicaSuccess)
		}
	}

	// Run concurrent operations
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			fd.updateNodeHealth("node1", i%2 == 0) // Use updateNodeHealth!
			fd.updateNodeHealth("node2", i%3 == 0)
			time.Sleep(10 * time.Millisecond)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			rm.checkAndRebalance(ctx) // Trigger rebalancing
			time.Sleep(10 * time.Millisecond)
		}
		done <- true
	}()

	// Wait for operations to complete
	<-done
	<-done

	// Add a brief sleep to allow any pending goroutines to finish
	time.Sleep(100 * time.Millisecond)
}
