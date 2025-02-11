package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const (
	defaultHeartbeatInterval = 5 * time.Second
	defaultFailureThreshold  = 3
	defaultCleanupInterval   = 30 * time.Second
)

// NodeHealth represents the current health status of a node
type NodeHealth struct {
	LastHeartbeat time.Time
	MissedBeats   int
	IsHealthy     bool
	Address       string
}

// FailureDetector manages node health monitoring
type FailureDetector struct {
	mu              sync.RWMutex
	nodes           map[string]*NodeHealth
	client          *http.Client
	interval        time.Duration
	threshold       int
	healthEndpoint  string
	stopChan        chan struct{}
	nodeUpdateChan  chan NodeUpdate
	CleanupInterval time.Duration
}

// NodeUpdate represents a node health status update
type NodeUpdate struct {
	NodeID    string
	IsHealthy bool
	Timestamp time.Time
}

// NewFailureDetector creates a new instance of FailureDetector
func NewFailureDetector(interval time.Duration, threshold int) *FailureDetector {
	if interval == 0 {
		interval = defaultHeartbeatInterval
	}
	if threshold == 0 {
		threshold = defaultFailureThreshold
	}

	fd := &FailureDetector{
		nodes:          make(map[string]*NodeHealth),
		client:         &http.Client{Timeout: interval / 3},
		interval:       interval,
		threshold:      threshold,
		healthEndpoint: "/health",
		stopChan:       make(chan struct{}),
		nodeUpdateChan: make(chan NodeUpdate, 100),
	}
	fd.CleanupInterval = 30 * time.Second
	return fd
}

// Start begins the heartbeat monitoring
func (fd *FailureDetector) Start(ctx context.Context) {
	ticker := time.NewTicker(fd.interval)
	cleanupTicker := time.NewTicker(fd.CleanupInterval)
	defer ticker.Stop()
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-fd.stopChan:
			return
		case <-ticker.C:
			fd.checkAllNodes()
		case <-cleanupTicker.C:
			fd.cleanup()
		case update := <-fd.nodeUpdateChan:
			fd.handleNodeUpdate(update)
		}
	}
}

// Stop stops the heartbeat monitoring
func (fd *FailureDetector) Stop() {
	select {
	case <-fd.stopChan:
		// Already stopped
		return
	default:
		close(fd.stopChan)
	}
}

// AddNode adds a node to be monitored
func (fd *FailureDetector) AddNode(nodeID, address string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	fd.nodes[nodeID] = &NodeHealth{
		LastHeartbeat: time.Now(),
		MissedBeats:   0,
		IsHealthy:     true,
		Address:       address,
	}
}

// RemoveNode removes a node from monitoring
func (fd *FailureDetector) RemoveNode(nodeID string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	delete(fd.nodes, nodeID)
}

// IsNodeHealthy checks if a node is considered healthy
func (fd *FailureDetector) IsNodeHealthy(nodeID string) bool {
	fd.mu.RLock()
	node, exists := fd.nodes[nodeID]
	isHealthy := exists && node.IsHealthy
	fd.mu.RUnlock()
	return isHealthy
}

// GetNodeHealth returns the health status of all nodes
func (fd *FailureDetector) GetNodeHealth() map[string]NodeHealth {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	health := make(map[string]NodeHealth)
	for nodeID, node := range fd.nodes {
		health[nodeID] = *node
	}
	return health
}

// checkAllNodes performs health checks on all registered nodes
func (fd *FailureDetector) checkAllNodes() {
	// First, get a snapshot of nodes to check
	fd.mu.RLock()
	nodesToCheck := make(map[string]string) // map[nodeID]address
	for nodeID, node := range fd.nodes {
		nodesToCheck[nodeID] = node.Address
	}
	fd.mu.RUnlock()

	// Check each node's health without holding the lock
	results := make(map[string]bool)
	for nodeID, address := range nodesToCheck {
		results[nodeID] = fd.checkNodeHealth(nodeID, address)
	}

	// Update node health statuses with results
	fd.mu.Lock()
	defer fd.mu.Unlock()

	for nodeID, isHealthy := range results {
		node, exists := fd.nodes[nodeID]
		if !exists {
			continue
		}

		if !isHealthy {
			node.MissedBeats++
			if node.MissedBeats >= fd.threshold && node.IsHealthy {
				node.IsHealthy = false
				select {
				case fd.nodeUpdateChan <- NodeUpdate{
					NodeID:    nodeID,
					IsHealthy: false,
					Timestamp: time.Now(),
				}:
				default:
					// Channel is full, skip update
				}
			}
		} else {
			if !node.IsHealthy {
				node.IsHealthy = true
				select {
				case fd.nodeUpdateChan <- NodeUpdate{
					NodeID:    nodeID,
					IsHealthy: true,
					Timestamp: time.Now(),
				}:
				default:
					// Channel is full, skip update
				}
			}
			node.MissedBeats = 0
			node.LastHeartbeat = time.Now()
		}
	}
}

// checkNodeHealth performs a health check on a single node
func (fd *FailureDetector) checkNodeHealth(nodeID string, address string) bool {
	// Create request with timeout
	ctx, cancel := context.WithTimeout(context.Background(), fd.interval/3)
	defer cancel()

	// Create request using node address
	url := fmt.Sprintf("http://%s%s", address, fd.healthEndpoint)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}

	// Perform health check
	resp, err := fd.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	// Consider any non-2xx status code as unhealthy
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// updateNodeHealth updates a node's health status
func (fd *FailureDetector) updateNodeHealth(nodeID string, isHealthy bool) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if node, exists := fd.nodes[nodeID]; exists {
		wasHealthy := node.IsHealthy
		if !isHealthy {
			node.MissedBeats++
			if node.MissedBeats >= fd.threshold {
				node.IsHealthy = false
			}
		} else {
			node.MissedBeats = 0
			node.IsHealthy = true
			node.LastHeartbeat = time.Now()
		}

		// Only send update if health status changed
		if wasHealthy != node.IsHealthy {
			select {
			case fd.nodeUpdateChan <- NodeUpdate{
				NodeID:    nodeID,
				IsHealthy: node.IsHealthy,
				Timestamp: time.Now(),
			}:
			default:
				// Channel is full, skip update
			}
		}
	}
}

// cleanup removes stale node entries
func (fd *FailureDetector) cleanup() {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	thresholdTime := time.Now().Add(-fd.CleanupInterval)
	for nodeID, node := range fd.nodes {
		if node.MissedBeats >= fd.threshold && node.LastHeartbeat.Before(thresholdTime) {
			delete(fd.nodes, nodeID)
		}
	}
}

// handleNodeUpdate processes node health updates
func (fd *FailureDetector) handleNodeUpdate(update NodeUpdate) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if node, exists := fd.nodes[update.NodeID]; exists {
		node.IsHealthy = update.IsHealthy
		node.LastHeartbeat = update.Timestamp
		if update.IsHealthy {
			node.MissedBeats = 0
		}
	}
}
