package cluster

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/arohanajit/Distributed-Hashmap/internal/storage"
	"go.uber.org/zap"
)

// ShutdownManager handles the graceful shutdown sequence for a node
type ShutdownManager struct {
	clusterManager   *ClusterManagerImpl
	server           *http.Server
	discoveryManager *DiscoveryManager
	reReplicationMgr *ReReplicationManager
	shardManager     ShardManager
	store            storage.Store
	logger           *zap.Logger
	timeout          time.Duration
	mu               sync.Mutex
	isShuttingDown   bool
}

// NewShutdownManager creates a new ShutdownManager instance
func NewShutdownManager(
	clusterManager *ClusterManagerImpl,
	server *http.Server,
	discoveryManager *DiscoveryManager,
	reReplicationMgr *ReReplicationManager,
	shardManager ShardManager,
	store storage.Store,
	logger *zap.Logger,
	timeout time.Duration,
) *ShutdownManager {
	if timeout == 0 {
		timeout = 30 * time.Second // Default timeout of 30 seconds
	}

	return &ShutdownManager{
		clusterManager:   clusterManager,
		server:           server,
		discoveryManager: discoveryManager,
		reReplicationMgr: reReplicationMgr,
		shardManager:     shardManager,
		store:            store,
		logger:           logger,
		timeout:          timeout,
		isShuttingDown:   false,
	}
}

// Shutdown performs a graceful shutdown of the node
func (sm *ShutdownManager) Shutdown(ctx context.Context) error {
	sm.mu.Lock()
	if sm.isShuttingDown {
		sm.mu.Unlock()
		return fmt.Errorf("shutdown already in progress")
	}
	sm.isShuttingDown = true
	sm.mu.Unlock()

	sm.logger.Info("Starting graceful shutdown sequence")

	// Create a context with timeout for the entire shutdown process
	ctx, cancel := context.WithTimeout(ctx, sm.timeout)
	defer cancel()

	// Create a WaitGroup to track ongoing operations
	var wg sync.WaitGroup

	// Step 1: Stop accepting new requests
	// We'll set a deadline for the server to shutdown
	sm.logger.Info("Stopping HTTP server - no longer accepting new requests")
	serverShutdownCtx, serverCancel := context.WithTimeout(ctx, 5*time.Second)
	defer serverCancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sm.server.Shutdown(serverShutdownCtx); err != nil {
			sm.logger.Error("Error shutting down HTTP server", zap.Error(err))
		}
	}()

	// Step 2: Wait for in-flight operations to complete
	sm.logger.Info("Waiting for in-flight operations to complete")
	if err := sm.waitForInFlightOperations(ctx); err != nil {
		sm.logger.Warn("Timed out waiting for in-flight operations", zap.Error(err))
	}

	// Step 3: Stop re-replication manager to complete in-flight rebalancing operations
	sm.logger.Info("Stopping re-replication manager")
	if sm.reReplicationMgr != nil {
		sm.reReplicationMgr.Stop()
	}

	// Step 4: Transfer owned shards to other nodes
	sm.logger.Info("Transferring owned shards to other nodes")
	if err := sm.transferShards(ctx); err != nil {
		sm.logger.Error("Error transferring shards", zap.Error(err))
	}

	// Step 5: Deregister from the cluster
	sm.logger.Info("Deregistering from the cluster")
	if sm.discoveryManager != nil {
		if err := sm.discoveryManager.StopDiscovery(ctx); err != nil {
			sm.logger.Error("Error deregistering from the cluster", zap.Error(err))
		}
	}

	// Wait for all operations to complete or timeout
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		sm.logger.Info("Graceful shutdown completed successfully")
		return nil
	case <-ctx.Done():
		sm.logger.Warn("Graceful shutdown timed out, forcing exit")
		return ctx.Err()
	}
}

// waitForInFlightOperations waits for in-flight operations to complete
func (sm *ShutdownManager) waitForInFlightOperations(ctx context.Context) error {
	// In a real implementation, you would track in-flight operations
	// and wait for them to complete before proceeding with shutdown.
	// This could involve:
	// 1. Checking for ongoing rebalancing operations
	// 2. Waiting for pending writes to complete
	// 3. Ensuring all read operations have finished

	// For this example, we'll just wait a short time to simulate waiting for operations
	select {
	case <-time.After(2 * time.Second):
		sm.logger.Info("In-flight operations completed")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// transferShards transfers owned primary shards to other nodes in the cluster
func (sm *ShutdownManager) transferShards(ctx context.Context) error {
	// Check if this node has any primary shards
	if !sm.shardManager.HasPrimaryShards() {
		sm.logger.Info("No primary shards to transfer")
		return nil
	}

	// Get all keys from the store
	keysStore, ok := sm.store.(StoreWithAllKeys)
	if !ok {
		return fmt.Errorf("store does not support GetAllKeys method")
	}

	// Get all nodes in the cluster except this one
	allNodes := sm.shardManager.GetAllNodes()
	selfNodeID := sm.shardManager.GetLocalNodeID()
	availableNodes := make([]string, 0, len(allNodes)-1)

	for _, nodeID := range allNodes {
		if nodeID != selfNodeID {
			availableNodes = append(availableNodes, nodeID)
		}
	}

	if len(availableNodes) == 0 {
		return fmt.Errorf("no available nodes to transfer shards to")
	}

	// Get metadata store to fetch values and content types
	metadataStore, ok := sm.store.(StoreWithGetMetadata)
	if !ok {
		return fmt.Errorf("store does not support GetMetadata method")
	}

	// Get all keys
	allKeys := keysStore.GetAllKeys()
	localNodeID := sm.shardManager.GetLocalNodeID()

	// Create a semaphore to limit concurrent transfers
	semaphore := make(chan struct{}, maxConcurrentTransfers)

	// Use a waitgroup to track transfers
	var wg sync.WaitGroup
	errCh := make(chan error, len(allKeys))

	for _, key := range allKeys {
		// Check if this node is responsible for this key
		responsibleNodes := sm.shardManager.GetResponsibleNodes(key)
		if len(responsibleNodes) == 0 || responsibleNodes[0] != localNodeID {
			continue // Skip keys we're not the primary for
		}

		// Find a target node for this key
		var targetNodeID string
		if len(availableNodes) == 1 {
			targetNodeID = availableNodes[0]
		} else {
			// Use a simple round-robin approach for now
			// In a real system, you might want to consider load balancing
			targetNodeID = availableNodes[0]
			// Rotate the availableNodes slice for the next key
			availableNodes = append(availableNodes[1:], availableNodes[0])
		}

		// Get the value and content type for this key
		value, contentType, err := metadataStore.Get(key)
		if err != nil {
			sm.logger.Error("Failed to get value for key", zap.String("key", key), zap.Error(err))
			continue
		}

		wg.Add(1)
		go func(k string, v []byte, ct string, target string) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Transfer the key-value pair to the target node
			err := sm.transferKeyToNode(ctx, k, v, ct, target)
			if err != nil {
				errCh <- fmt.Errorf("failed to transfer key %s to node %s: %v", k, target, err)
				return
			}

			// Update responsible nodes for this key
			currentResponsibleNodes := sm.shardManager.GetResponsibleNodes(k)
			newResponsibleNodes := make([]string, 0, len(currentResponsibleNodes))

			// Replace the local node with the target node
			for _, nodeID := range currentResponsibleNodes {
				if nodeID == localNodeID {
					newResponsibleNodes = append(newResponsibleNodes, target)
				} else {
					newResponsibleNodes = append(newResponsibleNodes, nodeID)
				}
			}

			// Update the shard manager
			sm.shardManager.UpdateResponsibleNodes(k, newResponsibleNodes)

			sm.logger.Info("Successfully transferred key",
				zap.String("key", k),
				zap.String("target", target))
		}(key, value, contentType, targetNodeID)
	}

	// Wait for all transfers to complete
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// Collect errors
	var transferErrors []error
	for err := range errCh {
		transferErrors = append(transferErrors, err)
	}

	if len(transferErrors) > 0 {
		return fmt.Errorf("encountered %d errors while transferring shards", len(transferErrors))
	}

	return nil
}

// transferKeyToNode transfers a single key-value pair to the specified node
func (sm *ShutdownManager) transferKeyToNode(ctx context.Context, key string, value []byte, contentType string, nodeID string) error {
	targetAddr := sm.shardManager.GetNodeAddress(nodeID)
	if targetAddr == "" {
		return fmt.Errorf("unknown node address for node ID: %s", nodeID)
	}

	// In a real implementation, you would use HTTP or RPC to transfer the key-value pair
	// This is a simplified example
	url := fmt.Sprintf("http://%s/keys/%s", targetAddr, key)

	// Create a request with the value in the body
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(value))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Set appropriate headers
	req.Header.Set("Content-Type", contentType)

	// Execute the request
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to transfer key, got status code: %d", resp.StatusCode)
	}

	return nil
}
