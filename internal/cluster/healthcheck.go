package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

const (
	defaultGossipInterval = 1 * time.Second
	defaultFanout         = 3
	suspectTTL            = 10 * time.Second
)

// GossipProtocol implements a gossip-based failure detector
type GossipProtocol struct {
	mu              sync.RWMutex
	nodes           map[string]string    // nodeID -> address
	suspected       map[string]time.Time // nodeID -> suspicion start time
	localNodeID     string
	client          *http.Client
	interval        time.Duration
	fanout          int
	stopChan        chan struct{}
	failureDetector *FailureDetector
}

// GossipMessage represents a health status update message
type GossipMessage struct {
	SenderID     string                `json:"sender_id"`
	Timestamp    time.Time             `json:"timestamp"`
	HealthStatus map[string]NodeHealth `json:"health_status"`
	Suspected    []string              `json:"suspected"`
}

// NewGossipProtocol creates a new instance of GossipProtocol
func NewGossipProtocol(localNodeID string, failureDetector *FailureDetector) *GossipProtocol {
	return &GossipProtocol{
		nodes:           make(map[string]string),
		suspected:       make(map[string]time.Time),
		localNodeID:     localNodeID,
		client:          &http.Client{Timeout: 2 * time.Second},
		interval:        defaultGossipInterval,
		fanout:          defaultFanout,
		stopChan:        make(chan struct{}),
		failureDetector: failureDetector,
	}
}

// Start begins the gossip protocol
func (gp *GossipProtocol) Start(ctx context.Context) {
	ticker := time.NewTicker(gp.interval)
	cleanupTicker := time.NewTicker(suspectTTL)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-gp.stopChan:
				ticker.Stop()
				cleanupTicker.Stop()
				return
			case <-ticker.C:
				gp.gossip()
			case <-cleanupTicker.C:
				gp.cleanupSuspected()
			}
		}
	}()
}

// Stop stops the gossip protocol
func (gp *GossipProtocol) Stop() {
	close(gp.stopChan)
}

// AddNode adds a node to the gossip network
func (gp *GossipProtocol) AddNode(nodeID, address string) {
	gp.mu.Lock()
	defer gp.mu.Unlock()
	gp.nodes[nodeID] = address
}

// RemoveNode removes a node from the gossip network
func (gp *GossipProtocol) RemoveNode(nodeID string) {
	gp.mu.Lock()
	defer gp.mu.Unlock()
	delete(gp.nodes, nodeID)
	delete(gp.suspected, nodeID)
}

// GetSuspectedNodes returns the list of currently suspected nodes
func (gp *GossipProtocol) GetSuspectedNodes() []string {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	suspected := make([]string, 0, len(gp.suspected))
	for nodeID := range gp.suspected {
		suspected = append(suspected, nodeID)
	}
	return suspected
}

// gossip sends health status to random subset of nodes
func (gp *GossipProtocol) gossip() {
	gp.mu.RLock()
	nodes := make([]string, 0, len(gp.nodes))
	for nodeID := range gp.nodes {
		if nodeID != gp.localNodeID {
			nodes = append(nodes, nodeID)
		}
	}
	gp.mu.RUnlock()

	if len(nodes) == 0 {
		return
	}

	// Select random subset of nodes
	targets := gp.selectRandomNodes(nodes, gp.fanout)
	message := gp.createGossipMessage()

	// Send gossip message to selected nodes
	for _, nodeID := range targets {
		go gp.sendGossipMessage(nodeID, message)
	}
}

// selectRandomNodes randomly selects n nodes from the given slice
func (gp *GossipProtocol) selectRandomNodes(nodes []string, n int) []string {
	if len(nodes) <= n {
		return nodes
	}

	selected := make([]string, n)
	indexes := rand.Perm(len(nodes))[:n]
	for i, idx := range indexes {
		selected[i] = nodes[idx]
	}
	return selected
}

// createGossipMessage creates a new gossip message with current health status
func (gp *GossipProtocol) createGossipMessage() GossipMessage {
	gp.mu.RLock()
	suspected := make([]string, 0, len(gp.suspected))
	for nodeID := range gp.suspected {
		suspected = append(suspected, nodeID)
	}
	gp.mu.RUnlock()

	return GossipMessage{
		SenderID:     gp.localNodeID,
		Timestamp:    time.Now(),
		HealthStatus: gp.failureDetector.GetNodeHealth(),
		Suspected:    suspected,
	}
}

// sendGossipMessage sends a gossip message to a specific node
func (gp *GossipProtocol) sendGossipMessage(nodeID string, message GossipMessage) {
	gp.mu.RLock()
	address := gp.nodes[nodeID]
	gp.mu.RUnlock()

	url := fmt.Sprintf("http://%s/cluster/gossip", address)
	body, err := json.Marshal(message)
	if err != nil {
		return
	}

	resp, err := gp.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		gp.markNodeSuspected(nodeID)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		gp.markNodeSuspected(nodeID)
	}
}

// markNodeSuspected marks a node as suspected
func (gp *GossipProtocol) markNodeSuspected(nodeID string) {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	if _, exists := gp.suspected[nodeID]; !exists {
		gp.suspected[nodeID] = time.Now()
	}
}

// cleanupSuspected removes expired suspected nodes
func (gp *GossipProtocol) cleanupSuspected() {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	now := time.Now()
	toRemove := make([]string, 0)

	// Collect nodes to remove
	for nodeID, timestamp := range gp.suspected {
		if now.Sub(timestamp) >= suspectTTL {
			toRemove = append(toRemove, nodeID)
		}
	}

	// Remove expired nodes
	for _, nodeID := range toRemove {
		delete(gp.suspected, nodeID)
	}
}

// ForceCleanup forces an immediate cleanup of suspected nodes
func (gp *GossipProtocol) ForceCleanup() {
	gp.cleanupSuspected()
}

// HandleGossipMessage processes incoming gossip messages
func (gp *GossipProtocol) HandleGossipMessage(message GossipMessage) {
	// Update local health status based on received information
	for nodeID, health := range message.HealthStatus {
		if nodeID != gp.localNodeID {
			gp.failureDetector.handleNodeUpdate(NodeUpdate{
				NodeID:    nodeID,
				IsHealthy: health.IsHealthy,
				Timestamp: health.LastHeartbeat,
			})
		}
	}

	// Update suspected nodes
	gp.mu.Lock()
	for _, nodeID := range message.Suspected {
		if _, exists := gp.suspected[nodeID]; !exists {
			gp.suspected[nodeID] = time.Now()
		}
	}
	gp.mu.Unlock()
}
