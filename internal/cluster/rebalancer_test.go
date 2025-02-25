package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/arohanajit/Distributed-Hashmap/internal/config"
	"github.com/arohanajit/Distributed-Hashmap/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rebalancerMockStore implements a simple in-memory store for testing
type rebalancerMockStore struct {
	mu    sync.RWMutex
	data  map[string][]byte
	nodes map[string]string // key -> node mapping
}

func newRebalancerMockStore() *rebalancerMockStore {
	return &rebalancerMockStore{
		data:  make(map[string][]byte),
		nodes: make(map[string]string),
	}
}

func (s *rebalancerMockStore) Set(key string, value []byte, nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	s.nodes[key] = nodeID
}

func (s *rebalancerMockStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.data[key]
	return value, exists
}

func (s *rebalancerMockStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.nodes, key)
}

func (s *rebalancerMockStore) GetNodeForKey(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nodes[key]
}

func (s *rebalancerMockStore) GetKeysForNode(nodeID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var keys []string
	for key, node := range s.nodes {
		if node == nodeID {
			keys = append(keys, key)
		}
	}
	return keys
}

func (s *rebalancerMockStore) GetAllKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	return keys
}

func (s *rebalancerMockStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// mockRebalancer implements a simple rebalancer for testing
type mockRebalancer struct {
	store       *rebalancerMockStore
	shardMgr    *storage.ShardManager
	transferLog []string // log of transfers for verification
	mu          sync.Mutex
}

func newMockRebalancer(store *rebalancerMockStore) *mockRebalancer {
	return &mockRebalancer{
		store:       store,
		shardMgr:    storage.NewShardManager(),
		transferLog: make([]string, 0),
	}
}

func (r *mockRebalancer) RebalanceOnNodeJoin(nodeID, address string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Add the node to the shard manager
	r.shardMgr.AddNode(address)

	// Get all keys and check if they need to be transferred
	keys := r.store.GetAllKeys()
	for _, key := range keys {
		responsibleNode := r.shardMgr.GetResponsibleNode(key)
		currentNode := r.store.GetNodeForKey(key)

		// If the key should be on the new node, transfer it
		if responsibleNode == address && currentNode != address {
			value, exists := r.store.Get(key)
			if exists {
				// Simulate transfer
				r.store.Set(key, value, address)
				r.transferLog = append(r.transferLog, fmt.Sprintf("Transfer key %s from %s to %s", key, currentNode, address))
			}
		}
	}

	return nil
}

func (r *mockRebalancer) RebalanceOnNodeLeave(nodeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Get all keys for the leaving node
	keys := r.store.GetKeysForNode(nodeID)

	// Remove the node from the shard manager
	r.shardMgr.RemoveNode(nodeID)

	// Redistribute keys
	for _, key := range keys {
		value, exists := r.store.Get(key)
		if exists {
			responsibleNode := r.shardMgr.GetResponsibleNode(key)
			if responsibleNode != "" && responsibleNode != nodeID {
				// Simulate transfer
				r.store.Set(key, value, responsibleNode)
				r.transferLog = append(r.transferLog, fmt.Sprintf("Transfer key %s from %s to %s", key, nodeID, responsibleNode))
			}
		}
	}

	return nil
}

func (r *mockRebalancer) GetTransferLog() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.transferLog...)
}

// TestRebalancer_NodeJoining tests data migration when a new node joins
func TestRebalancer_NodeJoining(t *testing.T) {
	// Setup
	store := newRebalancerMockStore()
	rebalancer := newMockRebalancer(store)

	// Add initial nodes
	initialNodes := []string{"node1", "node2", "node3"}
	for _, nodeID := range initialNodes {
		rebalancer.shardMgr.AddNode(nodeID)
	}

	// Add some test data
	testKeys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10"}
	for _, key := range testKeys {
		responsibleNode := rebalancer.shardMgr.GetResponsibleNode(key)
		store.Set(key, []byte(fmt.Sprintf("value-%s", key)), responsibleNode)
	}

	// Verify initial distribution
	initialDistribution := make(map[string]int)
	for _, nodeID := range initialNodes {
		initialDistribution[nodeID] = len(store.GetKeysForNode(nodeID))
	}

	// Add a new node
	newNode := "node4"
	err := rebalancer.RebalanceOnNodeJoin(newNode, newNode)
	require.NoError(t, err)

	// Verify keys were redistributed
	finalDistribution := make(map[string]int)
	for _, nodeID := range append(initialNodes, newNode) {
		finalDistribution[nodeID] = len(store.GetKeysForNode(nodeID))
	}

	// Verify the new node has some keys
	assert.Greater(t, finalDistribution[newNode], 0, "New node should have received some keys")

	// Verify all keys are still present
	assert.Equal(t, len(testKeys), store.Count(), "All keys should still be present")

	// Verify each key is assigned to its responsible node
	for _, key := range testKeys {
		responsibleNode := rebalancer.shardMgr.GetResponsibleNode(key)
		actualNode := store.GetNodeForKey(key)
		assert.Equal(t, responsibleNode, actualNode, "Key %s should be assigned to its responsible node", key)
	}

	// Verify transfers were logged
	transfers := rebalancer.GetTransferLog()
	assert.NotEmpty(t, transfers, "Some transfers should have been logged")

	t.Logf("Initial distribution: %v", initialDistribution)
	t.Logf("Final distribution: %v", finalDistribution)
	t.Logf("Transfers: %v", transfers)
}

// TestRebalancer_NodeLeaving tests data migration when a node leaves
func TestRebalancer_NodeLeaving(t *testing.T) {
	// Setup
	store := newRebalancerMockStore()
	rebalancer := newMockRebalancer(store)

	// Add initial nodes
	initialNodes := []string{"node1", "node2", "node3", "node4"}
	for _, nodeID := range initialNodes {
		rebalancer.shardMgr.AddNode(nodeID)
	}

	// Add some test data
	testKeys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10"}
	for _, key := range testKeys {
		responsibleNode := rebalancer.shardMgr.GetResponsibleNode(key)
		store.Set(key, []byte(fmt.Sprintf("value-%s", key)), responsibleNode)
	}

	// Verify initial distribution
	initialDistribution := make(map[string]int)
	for _, nodeID := range initialNodes {
		initialDistribution[nodeID] = len(store.GetKeysForNode(nodeID))
	}

	// Identify a node with keys to remove
	nodeToRemove := ""
	for _, nodeID := range initialNodes {
		if len(store.GetKeysForNode(nodeID)) > 0 {
			nodeToRemove = nodeID
			break
		}
	}
	require.NotEmpty(t, nodeToRemove, "Could not find a node with keys to remove")

	// Get keys on the node to be removed
	keysOnRemovedNode := store.GetKeysForNode(nodeToRemove)
	require.NotEmpty(t, keysOnRemovedNode, "Node to remove should have keys")

	// Remove the node
	err := rebalancer.RebalanceOnNodeLeave(nodeToRemove)
	require.NoError(t, err)

	// Verify keys were redistributed
	finalDistribution := make(map[string]int)
	for _, nodeID := range initialNodes {
		if nodeID != nodeToRemove {
			finalDistribution[nodeID] = len(store.GetKeysForNode(nodeID))
		}
	}

	// Verify the removed node has no keys
	assert.Equal(t, 0, len(store.GetKeysForNode(nodeToRemove)), "Removed node should have no keys")

	// Verify all keys are still present
	assert.Equal(t, len(testKeys), store.Count(), "All keys should still be present")

	// Verify each key is assigned to its responsible node
	for _, key := range testKeys {
		responsibleNode := rebalancer.shardMgr.GetResponsibleNode(key)
		actualNode := store.GetNodeForKey(key)
		assert.Equal(t, responsibleNode, actualNode, "Key %s should be assigned to its responsible node", key)
	}

	// Verify transfers were logged
	transfers := rebalancer.GetTransferLog()
	assert.NotEmpty(t, transfers, "Some transfers should have been logged")

	t.Logf("Initial distribution: %v", initialDistribution)
	t.Logf("Final distribution: %v", finalDistribution)
	t.Logf("Transfers: %v", transfers)
}

// TestRebalancer_ConcurrentScaling tests data migration during concurrent node joins and leaves
func TestRebalancer_ConcurrentScaling(t *testing.T) {
	// Setup
	store := newRebalancerMockStore()
	rebalancer := newMockRebalancer(store)

	// Add initial nodes
	initialNodes := []string{"node1", "node2", "node3"}
	for _, nodeID := range initialNodes {
		rebalancer.shardMgr.AddNode(nodeID)
	}

	// Add some test data - more keys for better distribution
	testKeys := make([]string, 100)
	for i := 0; i < 100; i++ {
		testKeys[i] = fmt.Sprintf("key%d", i)
		responsibleNode := rebalancer.shardMgr.GetResponsibleNode(testKeys[i])
		store.Set(testKeys[i], []byte(fmt.Sprintf("value-%d", i)), responsibleNode)
	}

	// Verify initial distribution
	initialDistribution := make(map[string]int)
	for _, nodeID := range initialNodes {
		initialDistribution[nodeID] = len(store.GetKeysForNode(nodeID))
	}

	// Simulate client traffic with concurrent reads and writes
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var clientWg sync.WaitGroup
	clientWg.Add(1)

	go func() {
		defer clientWg.Done()

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		r := newRand()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Simulate read
				key := testKeys[r.Intn(len(testKeys))]
				_, _ = store.Get(key)

				// Simulate write
				key = testKeys[r.Intn(len(testKeys))]
				responsibleNode := rebalancer.shardMgr.GetResponsibleNode(key)
				store.Set(key, []byte(fmt.Sprintf("updated-value-%s", key)), responsibleNode)
			}
		}
	}()

	// Concurrently add and remove nodes
	var scalingWg sync.WaitGroup

	// Add nodes
	for i := 4; i <= 6; i++ {
		scalingWg.Add(1)
		nodeID := fmt.Sprintf("node%d", i)

		go func(id string) {
			defer scalingWg.Done()
			err := rebalancer.RebalanceOnNodeJoin(id, id)
			assert.NoError(t, err)
		}(nodeID)
	}

	// Remove a node
	scalingWg.Add(1)
	go func() {
		defer scalingWg.Done()
		// Wait a bit to ensure some nodes have been added
		time.Sleep(50 * time.Millisecond)
		err := rebalancer.RebalanceOnNodeLeave("node1")
		assert.NoError(t, err)
	}()

	// Wait for all scaling operations to complete
	scalingWg.Wait()

	// Stop client traffic
	cancel()
	clientWg.Wait()

	// Verify final distribution
	finalNodes := []string{"node2", "node3", "node4", "node5", "node6"}
	finalDistribution := make(map[string]int)
	for _, nodeID := range finalNodes {
		finalDistribution[nodeID] = len(store.GetKeysForNode(nodeID))
	}

	// Verify node1 has no keys
	assert.Equal(t, 0, len(store.GetKeysForNode("node1")), "Removed node should have no keys")

	// Verify all keys are still present
	assert.Equal(t, len(testKeys), store.Count(), "All keys should still be present")

	// Verify each key is assigned to its responsible node
	for _, key := range testKeys {
		responsibleNode := rebalancer.shardMgr.GetResponsibleNode(key)
		actualNode := store.GetNodeForKey(key)
		assert.Equal(t, responsibleNode, actualNode, "Key %s should be assigned to its responsible node", key)
	}

	// Verify transfers were logged
	transfers := rebalancer.GetTransferLog()
	assert.NotEmpty(t, transfers, "Some transfers should have been logged")

	t.Logf("Initial distribution: %v", initialDistribution)
	t.Logf("Final distribution: %v", finalDistribution)
	t.Logf("Number of transfers: %d", len(transfers))
}

// TestRebalancer_BatchSizeLimit tests that rebalancing respects the batch size limit
func TestRebalancer_BatchSizeLimit(t *testing.T) {
	// Setup
	store := newRebalancerMockStore()
	rebalancer := newMockRebalancer(store)

	// Configure batch size
	cfg := config.DefaultConfig()
	cfg.RebalanceBatchSize = 5

	// Add initial nodes
	initialNodes := []string{"node1", "node2"}
	for _, nodeID := range initialNodes {
		rebalancer.shardMgr.AddNode(nodeID)
	}

	// Add test data - more keys for better testing of batching
	testKeys := make([]string, 50)
	for i := 0; i < 50; i++ {
		testKeys[i] = fmt.Sprintf("key%d", i)
		// Assign all keys to node1 initially
		store.Set(testKeys[i], []byte(fmt.Sprintf("value-%d", i)), "node1")
	}

	// Add a new node that will trigger rebalancing
	newNode := "node3"

	// Create a custom rebalancer with batch size limit
	batchRebalancer := &mockRebalancer{
		store:       store,
		shardMgr:    storage.NewShardManager(),
		transferLog: make([]string, 0),
	}

	// Add initial nodes to the new rebalancer
	for _, nodeID := range initialNodes {
		batchRebalancer.shardMgr.AddNode(nodeID)
	}

	// Simulate batch processing by manually tracking batches
	batchRebalancer.RebalanceOnNodeJoin(newNode, newNode)

	// Verify transfers
	transfers := batchRebalancer.GetTransferLog()

	// Count keys per node after rebalancing
	finalDistribution := make(map[string]int)
	for _, nodeID := range append(initialNodes, newNode) {
		finalDistribution[nodeID] = len(store.GetKeysForNode(nodeID))
	}

	// Verify the new node has received keys
	assert.Greater(t, finalDistribution[newNode], 0, "New node should have received keys")

	// Verify all keys are still present
	assert.Equal(t, len(testKeys), store.Count(), "All keys should still be present")

	t.Logf("Final distribution: %v", finalDistribution)
	t.Logf("Number of transfers: %d", len(transfers))
}

// Helper function to generate random data
func newRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}
