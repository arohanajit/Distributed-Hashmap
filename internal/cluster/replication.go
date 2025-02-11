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

const (
	maxRetries         = 3
	replicationTimeout = 10 * time.Second
	// ReplicationFactor is the number of replicas to maintain for each key
	ReplicationFactor = 3
	// defaultReplicationInterval is the interval between replication checks
	defaultReplicationInterval = 1 * time.Minute
)

// ReplicationManager handles data replication across nodes
type ReplicationManager struct {
	store           storage.Store // underlying store
	replicaMgr      *storage.ReplicaManager
	shardManager    ShardManager
	failureDetector *FailureDetector
	stopCh          chan struct{}
	interval        time.Duration
	wg              sync.WaitGroup
}

// NewReplicationManager creates a new instance of ReplicationManager
func NewReplicationManager(store storage.Store, shardManager ShardManager, failureDetector *FailureDetector) *ReplicationManager {
	return &ReplicationManager{
		store:           store,
		replicaMgr:      storage.NewReplicaManager(ReplicationFactor, (ReplicationFactor/2)+1),
		shardManager:    shardManager,
		failureDetector: failureDetector,
		stopCh:          make(chan struct{}),
		interval:        defaultReplicationInterval,
	}
}

// Replicator handles data replication across nodes
type Replicator struct {
	client     *http.Client
	replicaMgr *storage.ReplicaManager
	shardMgr   ShardManager
}

// NewReplicator creates a new instance of Replicator
func NewReplicator(replicaCount, writeQuorum int, shardMgr ShardManager) *Replicator {
	return &Replicator{
		client: &http.Client{
			Timeout: replicationTimeout,
		},
		replicaMgr: storage.NewReplicaManager(replicaCount, writeQuorum),
		shardMgr:   shardMgr,
	}
}

// ReplicateKey replicates data to successor nodes
func (r *Replicator) ReplicateKey(ctx context.Context, key string, data []byte, contentType string, primaryNode string) error {
	// Initialize replication tracking first
	requestID := utils.GenerateRequestID()
	responsibleNodes := r.shardMgr.GetResponsibleNodes(key)
	if len(responsibleNodes) == 0 {
		return fmt.Errorf("no responsible nodes found for key: %s", key)
	}

	// Initialize tracking with all responsible nodes
	tracker := r.replicaMgr.InitReplication(key, requestID, responsibleNodes)
	if tracker == nil {
		return fmt.Errorf("failed to initialize replication tracking for key: %s", key)
	}

	// Immediately mark primary node as successful
	if err := r.replicaMgr.UpdateReplicaStatus(key, primaryNode, storage.ReplicaSuccess); err != nil {
		return fmt.Errorf("failed to update primary node status: %v", err)
	}

	// Create wait group for concurrent replication
	var wg sync.WaitGroup
	errChan := make(chan error, len(responsibleNodes)-1)

	// Replicate to other nodes concurrently
	for _, nodeID := range responsibleNodes {
		if nodeID == primaryNode {
			continue
		}
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			if err := r.replicateToNode(ctx, key, data, contentType, nodeID, requestID); err != nil {
				errChan <- fmt.Errorf("failed to replicate to node %s: %v", nodeID, err)
				r.replicaMgr.UpdateReplicaStatus(key, nodeID, storage.ReplicaFailed)
			} else {
				r.replicaMgr.UpdateReplicaStatus(key, nodeID, storage.ReplicaSuccess)
			}
		}(nodeID)
	}

	wg.Wait()
	close(errChan)

	// Check if we achieved write quorum
	quorumMet, err := r.replicaMgr.CheckQuorum(key)
	if err != nil {
		return fmt.Errorf("failed to check quorum: %v", err)
	}

	if !quorumMet {
		return fmt.Errorf("failed to achieve write quorum")
	}

	return nil
}

// replicateToNode handles replication of data to a specific node
func (r *Replicator) replicateToNode(ctx context.Context, key string, value []byte, contentType string, nodeID string, requestID string) error {
	// Get node address
	nodeAddr := r.shardMgr.GetNodeAddress(nodeID)
	if nodeAddr == "" {
		return fmt.Errorf("no address found for node %s", nodeID)
	}

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 2 * time.Second, // Reduced from 10s to 2s
	}

	maxRetries := 2                      // Reduced from 3 to 2
	retryDelay := 500 * time.Millisecond // Reduced from 2s to 500ms

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		// Create request URL
		url := fmt.Sprintf("http://%s/keys/%s", nodeAddr, key)

		// Create request with context
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(value))
		if err != nil {
			lastErr = fmt.Errorf("failed to create request: %v", err)
			continue
		}

		// Set headers
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("X-Request-ID", requestID)

		// Send request
		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("failed to send request: %v", err)
			time.Sleep(retryDelay)
			continue
		}

		defer resp.Body.Close()

		// Check response status
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			time.Sleep(retryDelay)
			continue
		}

		// Success
		return nil
	}

	return fmt.Errorf("failed to replicate after %d retries: %v", maxRetries, lastErr)
}

// GetReplicaStatus returns the current status of replication for a key
func (r *Replicator) GetReplicaStatus(key string) (map[string]storage.ReplicaStatus, error) {
	return r.replicaMgr.GetReplicaStatus(key)
}

// GetReplicaNodes returns the list of nodes storing replicas for a key
func (rm *ReplicationManager) GetReplicaNodes(key string) []string {
	return rm.replicaMgr.GetReplicaNodes(key)
}

// RemoveReplication removes replication metadata for a key
func (rm *ReplicationManager) RemoveReplication(key string) {
	rm.replicaMgr.CleanupReplication(key)
}

// IsPendingReplication checks if a key is pending replication
func (rm *ReplicationManager) IsPendingReplication(key string) bool {
	return rm.replicaMgr.IsPendingReplication(key)
}
