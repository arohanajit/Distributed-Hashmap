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

func setupTestFailureDetector(t *testing.T) (*FailureDetector, context.Context, context.CancelFunc) {
	fd := NewFailureDetector(100*time.Millisecond, 2)
	ctx, cancel := context.WithCancel(context.Background())

	// Start failure detector in a goroutine
	go fd.Start(ctx)

	// Wait for failure detector to initialize
	time.Sleep(200 * time.Millisecond)

	return fd, ctx, cancel
}

func TestReReplicationManager_BasicReplication(t *testing.T) {
	// Setup failure detector with proper context
	fd, ctx, cancel := setupTestFailureDetector(t)
	defer cancel()

	// Create test servers for nodes, THIS IS IMPORTANT for reliable tests
	servers := make(map[string]*httptest.Server)
	for _, nodeID := range []string{"node1", "node2", "node3", "node4"} { // Include node4!
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut {
				w.WriteHeader(http.StatusCreated) // Simulate successful replication
			} else {
				w.WriteHeader(http.StatusMethodNotAllowed) // Or whatever is appropriate
			}
		}))
		defer server.Close()
		servers[nodeID] = server
	}

	// Create test components.  IMPORTANT: Use httptest.Server.URL for addresses.
	store := &mockStore{
		keys: map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
		},
	}

	replicaMgr := storage.NewReplicaManager(3, 2)
	shardMgr := &testShardManager{
		responsibleNodes: map[string][]string{
			"key1": {"node1", "node2", "node3"},
			"key2": {"node2", "node3", "node4"},
		},
		// Use the server URLs for node addresses.  This is key for proper test function.
		nodeAddresses: map[string]string{
			"node1": servers["node1"].URL[7:],
			"node2": servers["node2"].URL[7:],
			"node3": servers["node3"].URL[7:],
			"node4": servers["node4"].URL[7:],
		},
		currentNodeID: "node1",
	}

	// Create re-replication manager
	rm := NewReReplicationManager(store, replicaMgr, shardMgr, fd)

	// Add nodes to failure detector.  Use the addresses *from the shard manager*.
	for nodeID, addr := range shardMgr.nodeAddresses {
		fd.AddNode(nodeID, addr)
	}

	// Initialize replication tracking for key1 - This is crucial!
	replicaMgr.InitReplication("key1", "test-request", []string{"node1", "node2", "node3"})

	// Set initial replica states - IMPORTANT: Must match initial InitReplication state
	replicaMgr.UpdateReplicaStatus("key1", "node1", storage.ReplicaSuccess)
	replicaMgr.UpdateReplicaStatus("key1", "node2", storage.ReplicaSuccess) // Initially successful
	replicaMgr.UpdateReplicaStatus("key1", "node3", storage.ReplicaSuccess)

	// Mark node2 as unhealthy and wait for failure detector to process.
	fd.updateNodeHealth("node2", false) // Use the update function!
	time.Sleep(200 * time.Millisecond)  // Give failure detector time

	// Trigger re-replication check
	rm.checkAndRebalance(ctx)

	// Allow more time for re-replication, especially with network latency in tests.
	time.Sleep(500 * time.Millisecond)

	// Verify replica status.
	status, err := replicaMgr.GetReplicaStatus("key1")
	if err != nil {
		t.Errorf("Failed to get replica status: %v", err)
		return
	}

	// Debug output to help diagnose test failures
	t.Logf("Replica status after re-replication: %v", status)

	// Check if node2 is either marked as failed or no longer in the status map
	// The re-replication manager may remove failed nodes from the status map
	if val, exists := status["node2"]; exists && val != storage.ReplicaFailed {
		t.Errorf("Expected node2 to be marked as failed, got status: %v", val)
	}

	// Get updated responsible nodes to verify node4 is now included
	updatedNodes := shardMgr.GetResponsibleNodes("key1")
	t.Logf("Updated responsible nodes for key1: %v", updatedNodes)

	// Check node4 is now in the responsible nodes
	hasNode4 := false
	for _, nodeID := range updatedNodes {
		if nodeID == "node4" {
			hasNode4 = true
			break
		}
	}

	if !hasNode4 {
		t.Errorf("Expected node4, to be added to responsible nodes, got: %v", updatedNodes)
	}

	// Verify that a new node (e.g., node4) has a pending/success status,
	// depending on how fast re-replication happens.
	foundNewReplica := false
	for nodeID, s := range status {
		if nodeID != "node1" && nodeID != "node2" && nodeID != "node3" {
			if s == storage.ReplicaPending || s == storage.ReplicaSuccess {
				foundNewReplica = true
				break
			}
		}
	}
	if !foundNewReplica {
		t.Errorf("Expected a new replica to be created (pending or success), but none found. Status: %v", status)
	}
}

func TestFailureDetector_BasicOperations(t *testing.T) {
	fd := NewFailureDetector(time.Second, 2)

	// Test adding nodes
	fd.AddNode("node1", "localhost:8081")
	fd.AddNode("node2", "localhost:8082")

	// Test node health status
	if !fd.IsNodeHealthy("node1") {
		t.Error("Expected node1 to be healthy")
	}

	// Test removing nodes
	fd.RemoveNode("node1")
	if fd.IsNodeHealthy("node1") {
		t.Error("Expected node1 to be removed")
	}

	// Test getting node health
	health := fd.GetNodeHealth()
	if len(health) != 1 {
		t.Errorf("Expected 1 node in health map, got %d", len(health))
	}
}

func TestFailureDetector_HealthCheck(t *testing.T) {
	t.Log("Starting test")
	// Create test server
	serverUp := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Received request: %s %s", r.Method, r.URL.Path)
		if !serverUp {
			t.Log("Server is down, returning 503")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/health" {
			t.Log("Health check successful")
			w.WriteHeader(http.StatusOK)
		} else {
			t.Logf("Unknown path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	t.Logf("Test server started at %s", server.URL)

	// Create failure detector with shorter intervals for testing
	fd := NewFailureDetector(50*time.Millisecond, 2)
	fd.healthEndpoint = "/health"

	// Add test node
	nodeID := "test-node"
	fd.AddNode(nodeID, server.URL[7:]) // Strip "http://"
	t.Logf("Added node %s with address %s", nodeID, server.URL[7:])

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start failure detector in a goroutine
	t.Log("Starting failure detector")
	go fd.Start(ctx)
	defer fd.Stop()

	// Helper function to wait for condition with timeout
	waitForCondition := func(condition func() bool, timeout time.Duration, description string) error {
		t.Logf("Waiting for condition: %s", description)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if condition() {
				t.Logf("Condition met: %s", description)
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Logf("Condition timeout: %s", description)
		return fmt.Errorf("condition not met within timeout: %s", description)
	}

	// Wait for initial health check
	t.Log("Waiting for initial health check")
	if err := waitForCondition(func() bool {
		isHealthy := fd.IsNodeHealthy(nodeID)
		t.Logf("Health check result: %v", isHealthy)
		return isHealthy
	}, time.Second, "initial health check"); err != nil {
		t.Fatalf("Initial health check failed: %v", err)
	}

	// Simulate server failure
	t.Log("Simulating server failure")
	serverUp = false

	// Wait for failure detection
	if err := waitForCondition(func() bool {
		isHealthy := !fd.IsNodeHealthy(nodeID)
		t.Logf("Failure check result: %v", isHealthy)
		return isHealthy
	}, time.Second, "failure detection"); err != nil {
		t.Fatalf("Failure detection failed: %v", err)
	}

	// Restore server
	t.Log("Restoring server")
	serverUp = true

	// Wait for recovery detection
	if err := waitForCondition(func() bool {
		isHealthy := fd.IsNodeHealthy(nodeID)
		t.Logf("Recovery check result: %v", isHealthy)
		return isHealthy
	}, time.Second, "recovery detection"); err != nil {
		t.Fatalf("Recovery detection failed: %v", err)
	}

	t.Log("Test completed successfully")
}

func TestFailureDetector_Cleanup(t *testing.T) {
	fd := NewFailureDetector(100*time.Millisecond, 2)

	// Override the cleanup interval for fast testing
	fd.CleanupInterval = 250 * time.Millisecond

	// Add test nodes and mark them as unhealthy with old timestamps
	fd.AddNode("node1", "localhost:8081")
	fd.AddNode("node2", "localhost:8082")

	// Mark nodes as unhealthy with old timestamps
	fd.mu.Lock()
	for _, node := range fd.nodes {
		node.IsHealthy = false
		node.MissedBeats = fd.threshold
		node.LastHeartbeat = time.Now().Add(-fd.CleanupInterval * 2) // Set timestamp to well before cleanup threshold
	}
	fd.mu.Unlock()

	// Start failure detector in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fd.Start(ctx)

	// Wait for cleanup interval plus a buffer
	time.Sleep(350 * time.Millisecond)

	// Stop the failure detector
	fd.Stop()

	// Verify stale nodes are cleaned up
	health := fd.GetNodeHealth()
	if len(health) > 0 {
		t.Errorf("Expected all nodes to be cleaned up, got %d nodes", len(health))
	}
}

func TestFailureDetector_ConcurrentAccess(t *testing.T) {
	fd := NewFailureDetector(100*time.Millisecond, 2)
	done := make(chan bool)

	// Start concurrent operations
	go func() {
		for i := 0; i < 100; i++ {
			fd.AddNode("node1", "localhost:8081")
			fd.IsNodeHealthy("node1")
			fd.RemoveNode("node1")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			fd.GetNodeHealth()
		}
		done <- true
	}()

	// Wait for operations to complete
	<-done
	<-done
}

func TestFailureDetector_NodeFailureAndRecovery(t *testing.T) {
	// Create failure detector with shorter intervals for testing
	fd := NewFailureDetector(100*time.Millisecond, 2)

	// Create test server that can be controlled
	healthy := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Received request: %s %s", r.Method, r.URL.Path)
		if !healthy {
			t.Log("Server is down, returning 503")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		t.Log("Health check successful")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Logf("Test server started at %s", server.URL)

	// Add test nodes
	nodes := []string{"node1", "node2", "node3"}
	for _, nodeID := range nodes {
		fd.AddNode(nodeID, server.URL[7:])
		t.Logf("Added node %s with address %s", nodeID, server.URL[7:])
	}

	// Start failure detector in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Log("Starting failure detector")
	go fd.Start(ctx)
	defer fd.Stop()

	// Helper function to wait for condition with timeout
	waitForCondition := func(condition func() bool, timeout time.Duration, description string) error {
		t.Logf("Waiting for condition: %s", description)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if condition() {
				t.Logf("Condition met: %s", description)
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
		return fmt.Errorf("condition not met within timeout: %s", description)
	}

	// Wait for initial health check
	t.Log("Waiting for initial health check")
	if err := waitForCondition(func() bool {
		for _, nodeID := range nodes {
			if !fd.IsNodeHealthy(nodeID) {
				t.Logf("Node %s not healthy yet", nodeID)
				return false
			}
		}
		return true
	}, time.Second, "initial health check"); err != nil {
		t.Fatalf("Initial health check failed: %v", err)
	}

	// Simulate node failure
	t.Log("Simulating server failure")
	healthy = false

	// Wait for failure detection
	if err := waitForCondition(func() bool {
		for _, nodeID := range nodes {
			if fd.IsNodeHealthy(nodeID) {
				t.Logf("Node %s still healthy", nodeID)
				return false
			}
		}
		return true
	}, time.Second, "failure detection"); err != nil {
		t.Fatalf("Failure detection failed: %v", err)
	}

	// Restore server
	t.Log("Restoring server")
	healthy = true

	// Wait for recovery detection
	if err := waitForCondition(func() bool {
		for _, nodeID := range nodes {
			if !fd.IsNodeHealthy(nodeID) {
				t.Logf("Node %s not recovered yet", nodeID)
				return false
			}
		}
		return true
	}, time.Second, "recovery detection"); err != nil {
		t.Fatalf("Recovery detection failed: %v", err)
	}

	t.Log("Test completed successfully")
}

func TestFailureDetector_CascadingFailures(t *testing.T) {
	fd := NewFailureDetector(100*time.Millisecond, 2)

	// Create test servers
	servers := make(map[string]*httptest.Server)
	healthy := make(map[string]bool)

	for _, nodeID := range []string{"node1", "node2", "node3"} {
		healthy[nodeID] = true
		nodeIDCopy := nodeID // Capture for closure
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("Received request for node %s: %s %s", nodeIDCopy, r.Method, r.URL.Path)
			if !healthy[nodeIDCopy] {
				t.Logf("Node %s is unhealthy, returning 503", nodeIDCopy)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			t.Logf("Node %s is healthy, returning 200", nodeIDCopy)
			w.WriteHeader(http.StatusOK)
		}))
		servers[nodeID] = server
		fd.AddNode(nodeID, server.URL[7:])
		t.Logf("Added node %s with address %s", nodeID, server.URL[7:])
		defer server.Close()
	}

	// Start failure detector in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Log("Starting failure detector")
	go fd.Start(ctx)
	defer fd.Stop()

	// Helper function to wait for condition with timeout
	waitForCondition := func(condition func() bool, timeout time.Duration, description string) error {
		t.Logf("Waiting for condition: %s", description)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if condition() {
				t.Logf("Condition met: %s", description)
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
		return fmt.Errorf("condition not met within timeout: %s", description)
	}

	// Wait for initial health check
	t.Log("Waiting for initial health check")
	if err := waitForCondition(func() bool {
		for _, nodeID := range []string{"node1", "node2", "node3"} {
			if !fd.IsNodeHealthy(nodeID) {
				t.Logf("Node %s not healthy yet", nodeID)
				return false
			}
		}
		return true
	}, time.Second, "initial health check"); err != nil {
		t.Fatalf("Initial health check failed: %v", err)
	}

	// Simulate node failure
	t.Log("Simulating server failure")
	healthy["node2"] = false

	// Wait for failure detection - only node2 should be marked as unhealthy
	if err := waitForCondition(func() bool {
		node2Health := fd.IsNodeHealthy("node2")
		node1Health := fd.IsNodeHealthy("node1")
		node3Health := fd.IsNodeHealthy("node3")

		t.Logf("Health status - node1: %v, node2: %v, node3: %v",
			node1Health, node2Health, node3Health)

		// node2 should be unhealthy, others should be healthy
		return !node2Health && node1Health && node3Health
	}, time.Second, "failure detection"); err != nil {
		t.Fatalf("Failure detection failed: %v", err)
	}

	// Restore server
	t.Log("Restoring server")
	healthy["node2"] = true

	// Wait for recovery detection
	if err := waitForCondition(func() bool {
		for _, nodeID := range []string{"node1", "node2", "node3"} {
			if !fd.IsNodeHealthy(nodeID) {
				t.Logf("Node %s not recovered yet", nodeID)
				return false
			}
		}
		return true
	}, time.Second, "recovery detection"); err != nil {
		t.Fatalf("Recovery detection failed: %v", err)
	}

	t.Log("Test completed successfully")
}

func TestFailureDetector_NetworkPartition(t *testing.T) {
	fd := NewFailureDetector(100*time.Millisecond, 2)

	// Create test servers
	servers := make(map[string]*httptest.Server)
	partitioned := make(map[string]bool)

	for _, nodeID := range []string{"node1", "node2", "node3", "node4"} {
		partitioned[nodeID] = false
		nodeIDCopy := nodeID // Capture for closure
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("Received request for node %s: %s %s", nodeIDCopy, r.Method, r.URL.Path)
			if partitioned[nodeIDCopy] {
				t.Logf("Node %s is partitioned, returning 503", nodeIDCopy)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			t.Logf("Node %s is healthy, returning 200", nodeIDCopy)
			w.WriteHeader(http.StatusOK)
		}))
		servers[nodeID] = server
		fd.AddNode(nodeID, server.URL[7:])
		t.Logf("Added node %s with address %s", nodeID, server.URL[7:])
		defer server.Close()
	}

	// Start failure detector in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Log("Starting failure detector")
	go fd.Start(ctx)
	defer fd.Stop()

	// Helper function to wait for condition with timeout
	waitForCondition := func(condition func() bool, timeout time.Duration, description string) error {
		t.Logf("Waiting for condition: %s", description)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if condition() {
				t.Logf("Condition met: %s", description)
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
		return fmt.Errorf("condition not met within timeout: %s", description)
	}

	// Wait for initial health check
	t.Log("Waiting for initial health check")
	if err := waitForCondition(func() bool {
		for nodeID := range servers {
			if !fd.IsNodeHealthy(nodeID) {
				t.Logf("Node %s not healthy yet", nodeID)
				return false
			}
		}
		return true
	}, time.Second, "initial health check"); err != nil {
		t.Fatalf("Initial health check failed: %v", err)
	}

	// Simulate network partition
	t.Log("Simulating network partition for node1 and node2")
	partitioned["node1"] = true
	partitioned["node2"] = true

	// Wait for partition detection
	if err := waitForCondition(func() bool {
		for nodeID := range servers {
			expectedHealth := !partitioned[nodeID]
			currentHealth := fd.IsNodeHealthy(nodeID)
			t.Logf("Node %s - expected health: %v, current health: %v", nodeID, expectedHealth, currentHealth)
			if currentHealth != expectedHealth {
				return false
			}
		}
		return true
	}, time.Second, "partition detection"); err != nil {
		t.Fatalf("Partition detection failed: %v", err)
	}

	// Heal partition
	t.Log("Healing network partition")
	partitioned["node1"] = false
	partitioned["node2"] = false

	// Wait for recovery
	if err := waitForCondition(func() bool {
		for nodeID := range servers {
			if !fd.IsNodeHealthy(nodeID) {
				t.Logf("Node %s not recovered yet", nodeID)
				return false
			}
		}
		return true
	}, time.Second, "partition recovery"); err != nil {
		t.Fatalf("Partition recovery failed: %v", err)
	}

	t.Log("Test completed successfully")
}
