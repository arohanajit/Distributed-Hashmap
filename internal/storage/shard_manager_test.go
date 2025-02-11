package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestShardManager_BasicOperations tests basic node addition and removal
func TestShardManager_BasicOperations(t *testing.T) {
	sm := NewShardManager()

	// Test adding nodes
	nodes := []string{"node1:8080", "node2:8080", "node3:8080"}
	for _, node := range nodes {
		sm.AddNode(node)
	}

	// Verify all nodes are present
	allNodes := sm.GetAllNodes()
	if len(allNodes) != len(nodes) {
		t.Errorf("Expected %d nodes, got %d", len(nodes), len(allNodes))
	}

	// Sort both slices for comparison
	sort.Strings(allNodes)
	nodesCopy := make([]string, len(nodes))
	copy(nodesCopy, nodes)
	sort.Strings(nodesCopy)

	for i := range nodesCopy {
		if allNodes[i] != nodesCopy[i] {
			t.Errorf("Node mismatch at index %d: expected %s, got %s", i, nodesCopy[i], allNodes[i])
		}
	}

	// Test key assignment
	key := "test-key"
	node := sm.GetResponsibleNode(key)
	if node == "" {
		t.Error("Expected a responsible node, got empty string")
	}

	// Test key consistency
	node2 := sm.GetResponsibleNode(key)
	if node != node2 {
		t.Errorf("Key assignment not consistent: got %s then %s", node, node2)
	}

	// Test removing a node
	sm.RemoveNode(nodes[0])
	allNodes = sm.GetAllNodes()
	if len(allNodes) != len(nodes)-1 {
		t.Errorf("Expected %d nodes after removal, got %d", len(nodes)-1, len(allNodes))
	}
}

// TestShardManager_VirtualNodes tests virtual node functionality
func TestShardManager_VirtualNodes(t *testing.T) {
	virtualNodeCount := 10
	sm := NewShardManager(WithVirtualNodes(virtualNodeCount))

	node := "node1:8080"
	sm.AddNode(node)

	// Verify virtual nodes were created
	sm.mu.RLock()
	virtualNodes := sm.virtualNodes[node]
	sm.mu.RUnlock()

	if len(virtualNodes) != virtualNodeCount {
		t.Errorf("Expected %d virtual nodes, got %d", virtualNodeCount, len(virtualNodes))
	}

	// Verify all virtual nodes have unique hashes
	seen := make(map[uint32]bool)
	for _, hash := range virtualNodes {
		if seen[hash] {
			t.Errorf("Duplicate virtual node hash found: %d", hash)
		}
		seen[hash] = true
	}

	// Test distribution with virtual nodes
	keyCount := 1000
	distribution := make(map[string]int)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := sm.GetResponsibleNode(key)
		distribution[node]++
	}

	// With virtual nodes, we expect better distribution even with a single node
	if len(distribution) != 1 {
		t.Errorf("Expected all keys to go to the single node")
	}
}

// TestShardManager_Replication tests replication functionality
func TestShardManager_Replication(t *testing.T) {
	replicationFactor := 3
	sm := NewShardManager(WithReplication(replicationFactor))

	// Add several nodes
	nodes := []string{"node1:8080", "node2:8080", "node3:8080", "node4:8080", "node5:8080"}
	for _, node := range nodes {
		sm.AddNode(node)
	}

	// Test key replication
	key := "test-key"
	responsibleNodes := sm.GetResponsibleNodes(key)

	if len(responsibleNodes) != replicationFactor {
		t.Errorf("Expected %d responsible nodes, got %d", replicationFactor, len(responsibleNodes))
	}

	// Verify nodes are unique
	seen := make(map[string]bool)
	for _, node := range responsibleNodes {
		if seen[node] {
			t.Error("Duplicate node in responsible nodes")
		}
		seen[node] = true
	}

	// Test consistency
	responsibleNodes2 := sm.GetResponsibleNodes(key)
	if len(responsibleNodes2) != replicationFactor {
		t.Errorf("Expected %d responsible nodes on second call, got %d", replicationFactor, len(responsibleNodes2))
	}
	for i := range responsibleNodes {
		if responsibleNodes[i] != responsibleNodes2[i] {
			t.Errorf("Replication not consistent: got %v then %v", responsibleNodes, responsibleNodes2)
		}
	}
}

// TestShardManager_Distribution tests key distribution
func TestShardManager_Distribution(t *testing.T) {
	sm := NewShardManager()

	// Add nodes
	nodes := []string{"node1:8080", "node2:8080", "node3:8080"}
	for _, node := range nodes {
		sm.AddNode(node)
	}

	// Test distribution of keys
	keyCount := 10000
	distribution := make(map[string]int)

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := sm.GetResponsibleNode(key)
		distribution[node]++
	}

	// Check if keys are somewhat evenly distributed
	expectedPerNode := keyCount / len(nodes)
	tolerance := float64(expectedPerNode) * 0.15 // Allow 15% deviation

	for node, count := range distribution {
		diff := float64(abs(count - expectedPerNode))
		if diff > tolerance {
			t.Errorf("Uneven distribution for %s: got %d keys, expected %d ±%.0f",
				node, count, expectedPerNode, tolerance)
		}
	}
}

// TestShardManager_NodeFailure tests behavior when nodes fail
func TestShardManager_NodeFailure(t *testing.T) {
	sm := NewShardManager(WithReplication(2))

	// Add nodes
	nodes := []string{"node1:8080", "node2:8080", "node3:8080"}
	for _, node := range nodes {
		sm.AddNode(node)
	}

	// Get responsible nodes for multiple keys
	keys := []string{"key1", "key2", "key3"}
	originalAssignments := make(map[string][]string)
	for _, key := range keys {
		originalAssignments[key] = sm.GetResponsibleNodes(key)
	}

	// Remove a node and verify reassignment
	failedNode := nodes[0]
	sm.RemoveNode(failedNode)

	// Check reassignment for all keys
	for _, key := range keys {
		newNodes := sm.GetResponsibleNodes(key)
		original := originalAssignments[key]

		// If the failed node was responsible for this key
		wasResponsible := false
		for _, node := range original {
			if node == failedNode {
				wasResponsible = true
				break
			}
		}

		if wasResponsible {
			// Verify the key was reassigned properly
			if len(newNodes) != 2 {
				t.Errorf("Expected 2 responsible nodes after failure, got %d", len(newNodes))
			}
			// Verify the new assignment doesn't include the failed node
			for _, node := range newNodes {
				if node == failedNode {
					t.Error("Failed node still appears in assignments")
				}
			}
		}
	}
}

// TestShardManager_ConcurrentOperations tests concurrent node operations
func TestShardManager_ConcurrentOperations(t *testing.T) {
	sm := NewShardManager()
	var wg sync.WaitGroup

	// Concurrent node additions and removals
	nodeCount := 10
	operationCount := 100
	wg.Add(3) // For addition, removal, and lookup goroutines

	// Add nodes concurrently
	go func() {
		defer wg.Done()
		for i := 0; i < operationCount; i++ {
			node := fmt.Sprintf("node%d:8080", i%nodeCount)
			sm.AddNode(node)
			time.Sleep(time.Millisecond) // Small delay to increase contention
		}
	}()

	// Remove nodes concurrently
	go func() {
		defer wg.Done()
		for i := 0; i < operationCount; i++ {
			node := fmt.Sprintf("node%d:8080", i%nodeCount)
			sm.RemoveNode(node)
			time.Sleep(time.Millisecond)
		}
	}()

	// Lookup keys concurrently
	go func() {
		defer wg.Done()
		for i := 0; i < operationCount; i++ {
			key := fmt.Sprintf("key-%d", i)
			_ = sm.GetResponsibleNodes(key)
		}
	}()

	wg.Wait()
}

// TestShardManager_LoadBalancing tests load balancing with virtual nodes
func TestShardManager_LoadBalancing(t *testing.T) {
	// Test with different virtual node counts
	virtualNodeCounts := []int{1, 10, 100, 1000}
	nodeCount := 5
	keyCount := 10000

	// Expected maximum deviations for each virtual node count
	// These are based on empirical observations and decrease with more virtual nodes
	expectedDeviations := map[int]float64{
		1:    1.00, // Allow up to 100% deviation with 1 virtual node
		10:   0.50, // Allow up to 50% deviation with 10 virtual nodes
		100:  0.25, // Allow up to 25% deviation with 100 virtual nodes
		1000: 0.10, // Allow up to 10% deviation with 1000 virtual nodes
	}

	for _, vnodeCount := range virtualNodeCounts {
		t.Run(fmt.Sprintf("VirtualNodes=%d", vnodeCount), func(t *testing.T) {
			sm := NewShardManager(WithVirtualNodes(vnodeCount))

			// Add nodes
			for i := 0; i < nodeCount; i++ {
				sm.AddNode(fmt.Sprintf("node%d:8080", i))
			}

			// Distribute keys
			distribution := make(map[string]int)
			for i := 0; i < keyCount; i++ {
				key := fmt.Sprintf("key-%d", i)
				node := sm.GetResponsibleNode(key)
				distribution[node]++
			}

			// Calculate distribution metrics
			expectedPerNode := keyCount / nodeCount
			maxDev := 0
			for _, count := range distribution {
				dev := abs(count - expectedPerNode)
				if dev > maxDev {
					maxDev = dev
				}
			}

			// Get expected maximum deviation for this virtual node count
			maxAllowedDeviation := float64(expectedPerNode) * expectedDeviations[vnodeCount]
			deviationPercent := (float64(maxDev) * 100.0) / float64(expectedPerNode)

			if float64(maxDev) > maxAllowedDeviation {
				t.Errorf("Poor distribution with %d virtual nodes: max deviation %d (%.2f%% of expected), allowed: %.2f%%",
					vnodeCount, maxDev, deviationPercent, expectedDeviations[vnodeCount]*100)
			} else {
				t.Logf("Good distribution with %d virtual nodes: max deviation %d (%.2f%% of expected), allowed: %.2f%%",
					vnodeCount, maxDev, deviationPercent, expectedDeviations[vnodeCount]*100)
			}

			// Additional verification: check if distribution improves with more virtual nodes
			if vnodeCount > 1 {
				prevVnodeCount := virtualNodeCounts[sort.SearchInts(virtualNodeCounts, vnodeCount)-1]
				if deviationPercent >= (expectedDeviations[prevVnodeCount] * 100) {
					t.Errorf("Distribution did not improve significantly with more virtual nodes: "+
						"%d nodes: %.2f%% deviation >= %d nodes: %.2f%% expected",
						vnodeCount, deviationPercent, prevVnodeCount, expectedDeviations[prevVnodeCount]*100)
				}
			}
		})
	}
}

// TestShardManager_HashStability tests hash stability
func TestShardManager_HashStability(t *testing.T) {
	sm := NewShardManager()

	// Add initial nodes
	initialNodes := []string{"node1:8080", "node2:8080", "node3:8080"}
	for _, node := range initialNodes {
		sm.AddNode(node)
	}

	// Generate test keys
	testKeys := make([]string, 1000)
	for i := range testKeys {
		testKeys[i] = fmt.Sprintf("key-%d", i)
	}

	// Record initial assignments
	initialAssignments := make(map[string]string)
	for _, key := range testKeys {
		initialAssignments[key] = sm.GetResponsibleNode(key)
	}

	// Add a new node
	newNode := "node4:8080"
	sm.AddNode(newNode)

	// Check how many keys were reassigned
	reassignmentCount := 0
	for _, key := range testKeys {
		newAssignment := sm.GetResponsibleNode(key)
		if newAssignment != initialAssignments[key] {
			reassignmentCount++
		}
	}

	// With consistent hashing, we expect roughly 1/4 of keys to be reassigned
	expectedReassignment := len(testKeys) / 4
	tolerance := float64(expectedReassignment) * 0.2 // Allow 20% deviation
	if abs(reassignmentCount-expectedReassignment) > int(tolerance) {
		t.Errorf("Unexpected number of reassignments: got %d, expected %d ±%.0f",
			reassignmentCount, expectedReassignment, tolerance)
	}
}

// TestShardManager_Stress performs stress testing
func TestShardManager_Stress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	sm := NewShardManager()
	var wg sync.WaitGroup

	// Configuration
	duration := 5 * time.Second
	goroutines := 10
	nodes := []string{"node1:8080", "node2:8080", "node3:8080", "node4:8080", "node5:8080"}

	// Add initial nodes
	for _, node := range nodes {
		sm.AddNode(node)
	}

	// Start time
	start := time.Now()
	operations := int64(0)

	// Launch goroutines
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id)))

			for time.Since(start) < duration {
				// Random operation
				op := r.Intn(3)
				switch op {
				case 0: // Add node
					node := fmt.Sprintf("node%d:8080", r.Intn(10))
					sm.AddNode(node)
				case 1: // Remove node
					node := fmt.Sprintf("node%d:8080", r.Intn(10))
					sm.RemoveNode(node)
				case 2: // Get responsible nodes
					key := fmt.Sprintf("key-%d", r.Intn(1000))
					sm.GetResponsibleNodes(key)
				}
				atomic.AddInt64(&operations, 1)
			}
		}(i)
	}

	wg.Wait()
	t.Logf("Completed %d operations in %v", operations, duration)
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// --- Modified TestShardRebalancing ---
func TestShardRebalancing(t *testing.T) {
	// Create a new ShardManager and add initial nodes
	mgr := NewShardManager()
	for _, node := range []string{"node1", "node2", "node3"} {
		mgr.AddNode(node)
	}
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	initialOwnership := make(map[string]string)
	for _, key := range keys {
		owner := mgr.GetResponsibleNode(key)
		if owner == "" {
			t.Fatalf("No owner found for key %s", key)
		}
		initialOwnership[key] = owner
	}

	// Add a new node to trigger rebalancing
	mgr.AddNode("node4")
	for _, key := range keys {
		newOwner := mgr.GetResponsibleNode(key)
		if newOwner == "" {
			t.Errorf("Key %s has no owner after adding node4", key)
		}
		// Optionally, we could check if ownership has changed for at least one key
	}

	// Remove a node and ensure affected keys are reassigned
	mgr.RemoveNode("node2")
	for _, key := range keys {
		owner := mgr.GetResponsibleNode(key)
		if owner == "node2" {
			t.Errorf("Key %s is still assigned to removed node node2", key)
		}
	}
}

// --- Modified TestShardManagerConcurrency ---
func TestShardManagerConcurrency(t *testing.T) {
	mgr := NewShardManager()
	for _, node := range []string{"node1", "node2"} {
		mgr.AddNode(node)
	}
	var wg sync.WaitGroup
	keys := []string{"key1", "key2", "key3", "key4", "key5"}

	// Concurrently add nodes
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mgr.AddNode(fmt.Sprintf("node%d", i+3))
		}(i)
	}

	// Concurrently access key ownership
	for _, key := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			_ = mgr.GetResponsibleNode(k)
		}(key)
	}

	wg.Wait()
}
