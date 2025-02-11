package cluster

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/arohanajit/Distributed-Hashmap/internal/storage"
	"github.com/arohanajit/Distributed-Hashmap/internal/utils"
)

// Helper interfaces for store methods
// These are used via type assertions.

type StoreWithAllKeys interface {
	GetAllKeys() []string
}

type StoreWithGetMetadata interface {
	Get(key string) ([]byte, string, error)
}

const (
	defaultReReplicationInterval = 1 * time.Minute
	maxConcurrentTransfers       = 5
)

// ReReplicationManager handles data rebalancing when nodes fail
// It uses a store (which must implement GetAllKeys and Get) to get keys
// and the ShardManager to know replica placement.

type ReReplicationManager struct {
	mu              sync.RWMutex
	store           storage.Store // underlying store
	replicaMgr      *storage.ReplicaManager
	shardManager    ShardManager
	failureDetector *FailureDetector
	stopCh          chan struct{}
	interval        time.Duration
	wg              sync.WaitGroup
}

// NewReReplicationManager creates a new instance of ReReplicationManager
func NewReReplicationManager(store storage.Store, replicaMgr *storage.ReplicaManager, shardMgr ShardManager, failureDetector *FailureDetector) *ReReplicationManager {
	return &ReReplicationManager{
		store:           store,
		replicaMgr:      replicaMgr,
		shardManager:    shardMgr,
		failureDetector: failureDetector,
		stopCh:          make(chan struct{}),
		interval:        defaultReReplicationInterval,
	}
}

// NewReReplicationManagerWithInterval creates a new instance of ReReplicationManager with a custom interval
func NewReReplicationManagerWithInterval(store storage.Store, replicaMgr *storage.ReplicaManager, shardMgr ShardManager, failureDetector *FailureDetector, interval time.Duration) *ReReplicationManager {
	return &ReReplicationManager{
		store:           store,
		replicaMgr:      replicaMgr,
		shardManager:    shardMgr,
		failureDetector: failureDetector,
		stopCh:          make(chan struct{}),
		interval:        interval,
	}
}

// Start begins the re-replication process
func (rm *ReReplicationManager) Start(ctx context.Context) {
	ticker := time.NewTicker(rm.interval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-rm.stopCh:
				ticker.Stop()
				return
			case <-ticker.C:
				rm.checkAndRebalance(ctx)
			}
		}
	}()
}

// Stop stops the re-replication process
func (rm *ReReplicationManager) Stop() {
	close(rm.stopCh)
}

// Helper function to check if a slice contains a string
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// checkAndRebalance identifies and re-replicates data from unhealthy nodes
func (rm *ReReplicationManager) checkAndRebalance(ctx context.Context) {
	storeWithKeys, ok := interface{}(rm.store).(StoreWithAllKeys)
	if !ok {
		fmt.Printf("Store does not implement StoreWithAllKeys interface\n")
		return
	}

	keys := storeWithKeys.GetAllKeys()
	fmt.Printf("Checking %d keys for rebalancing\n", len(keys))

	for _, key := range keys {
		nodes := rm.shardManager.GetResponsibleNodes(key)
		fmt.Printf("Key %s has responsible nodes: %v\n", key, nodes)

		if len(nodes) == 0 {
			fmt.Printf("No responsible nodes for key %s, skipping\n", key)
			continue
		}

		// First check and update health status
		unhealthyCount := 0
		healthyNodes := make([]string, 0)
		unhealthyNodes := make([]string, 0)

		for _, nodeID := range nodes {
			isHealthy := rm.failureDetector.IsNodeHealthy(nodeID)
			fmt.Printf("Node %s health status: %v\n", nodeID, isHealthy)

			if !isHealthy {
				unhealthyCount++
				unhealthyNodes = append(unhealthyNodes, nodeID)
				// Mark node as failed in replica manager
				rm.replicaMgr.UpdateReplicaStatus(key, nodeID, storage.ReplicaFailed)
				fmt.Printf("Marked node %s as failed for key %s\n", nodeID, key)
			} else {
				healthyNodes = append(healthyNodes, nodeID)
			}
		}

		fmt.Printf("Found %d unhealthy nodes and %d healthy nodes for key %s\n", unhealthyCount, len(healthyNodes), key)

		if unhealthyCount == 0 {
			continue
		}

		// Get all available nodes from shard manager
		allNodes := rm.shardManager.GetAllNodes()
		availableNodes := make([]string, 0)

		// Find nodes that are not already responsible for this key and are healthy
		for _, nodeID := range allNodes {
			if !contains(nodes, nodeID) && rm.failureDetector.IsNodeHealthy(nodeID) {
				availableNodes = append(availableNodes, nodeID)
			}
		}

		// Calculate how many new replicas we need
		neededReplicas := rm.replicaMgr.ReplicaCount() - len(healthyNodes)
		fmt.Printf("Need %d new replicas (total: %d, current healthy: %d)\n",
			neededReplicas, rm.replicaMgr.ReplicaCount(), len(healthyNodes))

		if neededReplicas <= 0 {
			fmt.Printf("No new replicas needed, skipping\n")
			continue
		}

		// Select new nodes for re-replication
		newNodes := availableNodes
		if len(newNodes) > neededReplicas {
			newNodes = newNodes[:neededReplicas]
		}
		fmt.Printf("Selected new nodes for re-replication: %v\n", newNodes)

		// Update responsible nodes list with healthy nodes and new nodes
		updatedResponsibleNodes := append(healthyNodes, newNodes...)
		rm.shardManager.UpdateResponsibleNodes(key, updatedResponsibleNodes)
		fmt.Printf("Final responsible nodes after update: %v\n", updatedResponsibleNodes)

		// Generate a new request ID for re-replication
		requestID := utils.GenerateRequestID()

		// Initialize replication tracking for the new nodes
		rm.replicaMgr.InitReplication(key, requestID, updatedResponsibleNodes)

		// Get the data to replicate
		storeWithGet, ok := interface{}(rm.store).(StoreWithGetMetadata)
		if !ok {
			fmt.Printf("Store does not implement StoreWithGetMetadata interface\n")
			continue
		}

		data, contentType, err := storeWithGet.Get(key)
		if err != nil {
			fmt.Printf("Error getting data for key %s: %v\n", key, err)
			continue
		}

		// Create a replicator for this operation
		replicator := NewReplicator(rm.replicaMgr.ReplicaCount(), (rm.replicaMgr.ReplicaCount()/2)+1, rm.shardManager)

		// Replicate to new nodes
		err = replicator.ReplicateKey(ctx, key, data, contentType, healthyNodes[0])
		if err != nil {
			fmt.Printf("Error replicating key %s: %v\n", key, err)
			continue
		}
	}
}

// reReplicateKey identifies and re-replicates data for a specific key
func (rm *ReReplicationManager) reReplicateKey(ctx context.Context, key string, newNodes []string) {
	// Get current value and content type
	getter, ok := interface{}(rm.store).(StoreWithGetMetadata)
	if !ok {
		fmt.Printf("store does not implement StoreWithGetMetadata\n")
		return
	}

	// Get current value and content type
	value, contentType, err := getter.Get(key)
	if err != nil {
		fmt.Printf("failed to get value for key %s: %v\n", key, err)
		return
	}

	// Get the request ID from the tracker
	tracker, exists := rm.replicaMgr.GetTracker(key)
	if !exists {
		fmt.Printf("no tracker found for key %s\n", key)
		return
	}

	// Replicate to new nodes concurrently
	var wg sync.WaitGroup
	for _, nodeID := range newNodes {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			if err := rm.replicateToNode(ctx, key, value, contentType, nodeID, tracker.RequestID); err != nil {
				fmt.Printf("failed to replicate to node %s: %v\n", nodeID, err)
				rm.replicaMgr.UpdateReplicaStatus(key, nodeID, storage.ReplicaFailed)
			} else {
				rm.replicaMgr.UpdateReplicaStatus(key, nodeID, storage.ReplicaSuccess)
			}
		}(nodeID)
	}

	wg.Wait()
}

// replicateToNode handles replication of data to a specific node
func (rm *ReReplicationManager) replicateToNode(ctx context.Context, key string, value []byte, contentType string, nodeID string, requestID string) error {
	// Get node address
	nodeAddr := rm.shardManager.GetNodeAddress(nodeID)
	if nodeAddr == "" {
		return fmt.Errorf("no address found for node %s", nodeID)
	}

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Create request URL
	url := fmt.Sprintf("http://%s/keys/%s", nodeAddr, key)

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(value))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Set headers
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Request-ID", requestID)

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Update replica status
	rm.replicaMgr.UpdateReplicaStatus(key, nodeID, storage.ReplicaSuccess)
	return nil
}
