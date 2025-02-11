package storage

import (
	"fmt"
	"sync"
	"time"
)

// ReplicaStatus represents the current state of a replica
type ReplicaStatus int

const (
	ReplicaPending ReplicaStatus = iota
	ReplicaSuccess
	ReplicaFailed
)

// ReplicaInfo tracks the status of a single replica
type ReplicaInfo struct {
	NodeID     string
	Status     ReplicaStatus
	Timestamp  time.Time
	RetryCount int
}

// ReplicaTracker tracks replication status for a key
type ReplicaTracker struct {
	RequestID string
	Replicas  map[string]*ReplicaInfo // nodeID -> replicaInfo
	mu        sync.RWMutex
}

// ReplicaManager handles tracking of replica locations and replication status
type ReplicaManager struct {
	trackers        map[string]*ReplicaTracker // key -> tracker
	mu              sync.RWMutex
	replicaCount    int
	writeQuorum     int
	replicaNodes    map[string][]string  // Maps key to list of node IDs storing replicas
	pendingReplicas map[string]time.Time // Tracks keys pending replication
}

// NewReplicaManager creates a new instance of ReplicaManager
func NewReplicaManager(replicaCount, writeQuorum int) *ReplicaManager {
	return &ReplicaManager{
		trackers:        make(map[string]*ReplicaTracker),
		replicaCount:    replicaCount,
		writeQuorum:     writeQuorum,
		replicaNodes:    make(map[string][]string),
		pendingReplicas: make(map[string]time.Time),
	}
}

// InitReplication initializes replication tracking for a key
func (rm *ReplicaManager) InitReplication(key, requestID string, nodes []string) *ReplicaTracker {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	tracker := &ReplicaTracker{
		RequestID: requestID,
		Replicas:  make(map[string]*ReplicaInfo),
	}

	// Initialize replica info for each node
	for _, nodeID := range nodes {
		tracker.Replicas[nodeID] = &ReplicaInfo{
			NodeID:    nodeID,
			Status:    ReplicaPending,
			Timestamp: time.Now(),
		}
	}

	rm.trackers[key] = tracker
	rm.replicaNodes[key] = nodes
	rm.pendingReplicas[key] = time.Now()
	return tracker
}

// UpdateReplicaStatus updates the status of a replica
func (rm *ReplicaManager) UpdateReplicaStatus(key, nodeID string, status ReplicaStatus) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	tracker, exists := rm.trackers[key]
	if !exists {
		return fmt.Errorf("no tracker found for key: %s", key)
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	replica, exists := tracker.Replicas[nodeID]
	if !exists {
		return fmt.Errorf("no replica info found for node: %s", nodeID)
	}

	replica.Status = status
	replica.Timestamp = time.Now()
	if status == ReplicaFailed {
		replica.RetryCount++
	}

	return nil
}

// CheckQuorum checks if the write quorum has been achieved for a key
func (rm *ReplicaManager) CheckQuorum(key string) (bool, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	tracker, exists := rm.trackers[key]
	if !exists {
		return false, fmt.Errorf("no tracker found for key: %s", key)
	}

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	successCount := 0
	for _, replica := range tracker.Replicas {
		if replica.Status == ReplicaSuccess {
			successCount++
		}
	}

	return successCount >= rm.writeQuorum, nil
}

// GetReplicaStatus gets the current status of replication for a key
func (rm *ReplicaManager) GetReplicaStatus(key string) (map[string]ReplicaStatus, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	tracker, exists := rm.trackers[key]
	if !exists {
		return nil, fmt.Errorf("no tracker found for key: %s", key)
	}

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	status := make(map[string]ReplicaStatus)
	for nodeID, replica := range tracker.Replicas {
		status[nodeID] = replica.Status
	}

	return status, nil
}

// CleanupReplication removes replication metadata for a key
func (rm *ReplicaManager) CleanupReplication(key string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	delete(rm.trackers, key)
	delete(rm.replicaNodes, key)
	delete(rm.pendingReplicas, key)
}

// GetBackoffDuration calculates the backoff duration for retries
func (rm *ReplicaManager) GetBackoffDuration(retryCount int) time.Duration {
	// Exponential backoff with a maximum of 1 minute
	baseDelay := time.Second
	maxDelay := time.Minute

	delay := baseDelay * time.Duration(1<<uint(retryCount))
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

// ReplicaCount returns the configured number of replicas
func (rm *ReplicaManager) ReplicaCount() int {
	return rm.replicaCount
}

// GetReplicaNodes returns the list of nodes storing replicas for a key
func (rm *ReplicaManager) GetReplicaNodes(key string) []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if nodes, exists := rm.replicaNodes[key]; exists {
		return append([]string{}, nodes...)
	}
	return nil
}

// IsPendingReplication checks if a key is pending replication
func (rm *ReplicaManager) IsPendingReplication(key string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	_, isPending := rm.pendingReplicas[key]
	return isPending
}

// GetTracker returns the replication tracker for the given key and a boolean indicating its existence.
func (rm *ReplicaManager) GetTracker(key string) (*ReplicaTracker, bool) {
	rm.mu.RLock()
	tracker, exists := rm.trackers[key]
	rm.mu.RUnlock()
	return tracker, exists
}

// SetTracker sets the replication tracker for the given key and updates the replica nodes and pending replica timestamp.
func (rm *ReplicaManager) SetTracker(key string, tracker *ReplicaTracker) {
	rm.mu.Lock()
	rm.trackers[key] = tracker
	// Update replicaNodes based on tracker's replicas
	var nodes []string
	tracker.mu.RLock()
	for nodeID := range tracker.Replicas {
		nodes = append(nodes, nodeID)
	}
	tracker.mu.RUnlock()
	rm.replicaNodes[key] = nodes
	rm.pendingReplicas[key] = time.Now()
	rm.mu.Unlock()
}

// AddReplica adds a new replica entry to the tracker.
func (rt *ReplicaTracker) AddReplica(nodeID string, status ReplicaStatus, timestamp time.Time) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.Replicas[nodeID] = &ReplicaInfo{
		NodeID:    nodeID,
		Status:    status,
		Timestamp: timestamp,
	}
}

// HasReplica checks if the tracker already has a replica entry for the given nodeID.
func (rt *ReplicaTracker) HasReplica(nodeID string) bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	_, exists := rt.Replicas[nodeID]
	return exists
}

// GetRequestID returns the request ID for a key
func (rm *ReplicaManager) GetRequestID(key string) string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if tracker, exists := rm.trackers[key]; exists {
		return tracker.RequestID
	}
	return ""
}

// AddNodeToTracker adds a new node to an existing tracker
func (rm *ReplicaManager) AddNodeToTracker(key, nodeID, requestID string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	tracker, exists := rm.trackers[key]
	if !exists {
		// If no tracker exists, create a new one
		tracker = &ReplicaTracker{
			RequestID: requestID,
			Replicas:  make(map[string]*ReplicaInfo),
		}
		rm.trackers[key] = tracker
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	// Add the new node to the tracker
	tracker.Replicas[nodeID] = &ReplicaInfo{
		NodeID:    nodeID,
		Status:    ReplicaPending,
		Timestamp: time.Now(),
	}

	// Update the list of responsible nodes
	nodes := rm.replicaNodes[key]
	found := false
	for _, n := range nodes {
		if n == nodeID {
			found = true
			break
		}
	}
	if !found {
		rm.replicaNodes[key] = append(rm.replicaNodes[key], nodeID)
	}

	return nil
}
