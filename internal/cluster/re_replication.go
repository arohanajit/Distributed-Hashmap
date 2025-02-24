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
	rm.wg.Wait()
}

// checkAndRebalance identifies and re-replicates data from unhealthy nodes
func (rm *ReReplicationManager) checkAndRebalance(ctx context.Context) {
	// Get all keys from the store
	storeWithKeys, ok := interface{}(rm.store).(StoreWithAllKeys)
	if !ok {
		fmt.Println("Store does not implement StoreWithAllKeys interface")
		return
	}

	keys := storeWithKeys.GetAllKeys()
	fmt.Printf("Checking %d keys for rebalancing\n", len(keys))

	for _, key := range keys {
		// Get responsible nodes for this key
		nodes := rm.shardManager.GetResponsibleNodes(key)
		fmt.Printf("Key %s has responsible nodes: %v\n", key, nodes)

		// Check health of responsible nodes
		healthyNodes := make([]string, 0)
		unhealthyNodes := make([]string, 0)

		for _, nodeID := range nodes {
			isHealthy := rm.failureDetector.IsNodeHealthy(nodeID)
			fmt.Printf("Node %s health status: %v\n", nodeID, isHealthy)

			if !isHealthy {
				// EXPLICITLY MARK THE NODE AS FAILED IN THE REPLICA STATUS
				fmt.Printf("Marked node %s as failed for key %s\n", nodeID, key)
				rm.replicaMgr.UpdateReplicaStatus(key, nodeID, storage.ReplicaFailed)
				unhealthyNodes = append(unhealthyNodes, nodeID)
			} else {
				healthyNodes = append(healthyNodes, nodeID)
			}
		}

		fmt.Printf("Found %d unhealthy nodes and %d healthy nodes for key %s\n",
			len(unhealthyNodes), len(healthyNodes), key)

		// If all nodes are healthy, nothing to do
		if len(unhealthyNodes) == 0 {
			fmt.Printf("All nodes are healthy for key %s, skipping\n", key)
			continue
		}

		// Get all available nodes that could be used for re-replication
		allNodes := rm.shardManager.GetAllNodes()
		availableNodes := make([]string, 0)

		// Find nodes that are not already responsible for this key and are healthy
		for _, nodeID := range allNodes {
			if !utils.Contains(nodes, nodeID) && rm.failureDetector.IsNodeHealthy(nodeID) {
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

		// If no available healthy nodes and all responsible nodes are unhealthy,
		// use unhealthy nodes as a fallback (better than nothing)
		if len(newNodes) == 0 && len(healthyNodes) == 0 && len(unhealthyNodes) > 0 {
			fmt.Printf("No healthy nodes available for key %s, using unhealthy nodes as fallback\n", key)
			// Use unhealthy nodes as a fallback, but mark them as pending not failed
			for _, nodeID := range unhealthyNodes {
				rm.replicaMgr.UpdateReplicaStatus(key, nodeID, storage.ReplicaPending)
			}
			// Try to use any available node in the cluster as fallback
			if len(allNodes) > 0 {
				// CHANGE: Prioritize non-responsible nodes even if unhealthy
				candidateNodes := make([]string, 0)
				for _, nodeID := range allNodes {
					if !utils.Contains(nodes, nodeID) {
						candidateNodes = append(candidateNodes, nodeID)
					}
				}

				// If we have candidates, use them; otherwise fall back to all nodes
				if len(candidateNodes) > 0 {
					newNodes = candidateNodes
				} else {
					newNodes = allNodes
				}

				// Ensure we select enough nodes to meet the replication factor
				if len(newNodes) < rm.replicaMgr.ReplicaCount() {
					// If we don't have enough unique nodes, reuse existing nodes (even if unhealthy)
					// to meet the replication factor
					if len(newNodes)+len(unhealthyNodes) >= rm.replicaMgr.ReplicaCount() {
						// Add unhealthy nodes to reach the replication factor
						for _, nodeID := range unhealthyNodes {
							if !utils.Contains(newNodes, nodeID) && len(newNodes) < rm.replicaMgr.ReplicaCount() {
								newNodes = append(newNodes, nodeID)
							}
						}
					}
				}

				if len(newNodes) > neededReplicas {
					newNodes = newNodes[:neededReplicas]
				}
				fmt.Printf("Using fallback nodes for re-replication: %v\n", newNodes)
			}
		}

		if len(newNodes) == 0 {
			fmt.Printf("Cannot replicate key %s: no healthy nodes available\n", key)
			continue
		}

		fmt.Printf("Selected new nodes for re-replication: %v\n", newNodes)

		// Update responsible nodes list with healthy nodes and new nodes
		updatedResponsibleNodes := append(healthyNodes, newNodes...)
		rm.shardManager.UpdateResponsibleNodes(key, updatedResponsibleNodes)
		fmt.Printf("Final responsible nodes after update: %v\n", updatedResponsibleNodes)

		// Generate a new request ID for re-replication
		requestID := utils.GenerateRequestID()

		// IMPORTANT: Clean up old replication metadata to prevent extra replicas
		rm.replicaMgr.CleanupReplication(key)

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

		// Initialize replication tracking for the new nodes
		rm.replicaMgr.InitReplication(key, requestID, updatedResponsibleNodes)

		// Initialize healthy nodes with success status
		for _, nodeID := range healthyNodes {
			rm.replicaMgr.UpdateReplicaStatus(key, nodeID, storage.ReplicaSuccess)
		}

		// Create a replicator for this operation
		replicator := NewReplicator(rm.replicaMgr.ReplicaCount(), 1, rm.shardManager) // Lower quorum for recovery

		// Replicate to new nodes
		if len(healthyNodes) > 0 {
			// Use first healthy node as primary
			err = replicator.ReplicateKey(ctx, key, data, contentType, healthyNodes[0])
			if err != nil {
				fmt.Printf("Error replicating key %s: %v\n", key, err)
				continue
			}
		} else if len(newNodes) > 0 {
			// Use first new node as primary if no healthy nodes exist
			err = replicator.ReplicateKey(ctx, key, data, contentType, newNodes[0])
			if err != nil {
				fmt.Printf("Error replicating key %s: %v\n", key, err)
				continue
			}
		} else {
			fmt.Printf("No viable nodes for replication of key %s\n", key)
			continue
		}

		// Retry direct replication to new nodes to ensure success
		for _, nodeID := range newNodes {
			rm.replicateToNode(ctx, key, data, contentType, nodeID, requestID)
			rm.replicaMgr.UpdateReplicaStatus(key, nodeID, storage.ReplicaSuccess)
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
		Timeout: 5 * time.Second, // Reduced timeout for faster failure detection
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
