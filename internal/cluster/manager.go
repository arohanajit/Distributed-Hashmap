package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ClusterManager interface defines methods for managing cluster nodes
type ClusterManager interface {
	JoinNode(ctx context.Context, node Node) error
	LeaveNode(ctx context.Context, nodeID string) error
	GetNode(ctx context.Context, nodeID string) (*Node, error)
	ListNodes(ctx context.Context) ([]Node, error)
}

// ClusterManagerImpl implements ClusterManager interface
type ClusterManagerImpl struct {
	nodes            map[string]Node
	shardManager     ShardManager
	healthChecker    HealthChecker
	failureDetector  *FailureDetector
	gossipProtocol   *GossipProtocol
	client           *http.Client
	mu               sync.RWMutex
	discoveryManager *DiscoveryManager
	swimGossip       *SwimGossip
}

// NewClusterManager creates a new ClusterManagerImpl instance
func NewClusterManager(shardManager ShardManager, healthChecker HealthChecker, failureDetector *FailureDetector, gossipProtocol *GossipProtocol) *ClusterManagerImpl {
	return &ClusterManagerImpl{
		nodes:           make(map[string]Node),
		shardManager:    shardManager,
		healthChecker:   healthChecker,
		failureDetector: failureDetector,
		gossipProtocol:  gossipProtocol,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// NewClusterManagerWithDiscovery creates a new ClusterManagerImpl with etcd discovery
func NewClusterManagerWithDiscovery(self Node, shardManager ShardManager, healthChecker HealthChecker) (*ClusterManagerImpl, error) {
	// Initialize failure detector
	failureDetector := NewFailureDetector(defaultHeartbeatInterval, defaultFailureThreshold)

	// Initialize discovery manager
	discoveryManager := NewDiscoveryManager(self)

	// Create cluster manager
	cm := &ClusterManagerImpl{
		nodes:            make(map[string]Node),
		shardManager:     shardManager,
		healthChecker:    healthChecker,
		failureDetector:  failureDetector,
		discoveryManager: discoveryManager,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	// Set the cluster manager in the discovery manager
	discoveryManager.SetClusterManager(cm)

	// Load initial nodes from environment
	if err := discoveryManager.LoadNodesFromEnv(); err != nil {
		return nil, err
	}

	return cm, nil
}

// NewClusterManagerWithSwim creates a new ClusterManagerImpl with SWIM gossip
func NewClusterManagerWithSwim(self Node, shardManager ShardManager, healthChecker HealthChecker) (*ClusterManagerImpl, error) {
	// Initialize failure detector
	failureDetector := NewFailureDetector(defaultHeartbeatInterval, defaultFailureThreshold)

	// Initialize SWIM gossip
	swimGossip, err := NewSwimGossip(self)
	if err != nil {
		return nil, fmt.Errorf("failed to create SWIM gossip: %v", err)
	}

	// Create cluster manager
	cm := &ClusterManagerImpl{
		nodes:           make(map[string]Node),
		shardManager:    shardManager,
		healthChecker:   healthChecker,
		failureDetector: failureDetector,
		swimGossip:      swimGossip,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	// Set the cluster manager in the SWIM gossip
	swimGossip.SetClusterManager(cm)

	return cm, nil
}

// StartDiscovery starts the etcd-based discovery service
func (cm *ClusterManagerImpl) StartDiscovery(ctx context.Context, config DiscoveryConfig) error {
	if cm.discoveryManager == nil {
		return fmt.Errorf("discovery manager not initialized")
	}

	// Connect to etcd
	if err := cm.discoveryManager.ConnectToEtcd(config); err != nil {
		return err
	}

	// Register self with etcd
	if err := cm.discoveryManager.RegisterWithEtcd(ctx); err != nil {
		return err
	}

	return nil
}

// StartSwim starts the SWIM gossip protocol
func (cm *ClusterManagerImpl) StartSwim(ctx context.Context, config SwimConfig) error {
	if cm.swimGossip == nil {
		return fmt.Errorf("SWIM gossip not initialized")
	}

	// Set config if provided
	if config != (SwimConfig{}) {
		cm.swimGossip.SetConfig(config)
	}

	// Start SWIM protocol
	if err := cm.swimGossip.Start(ctx); err != nil {
		return err
	}

	return nil
}

// StopDiscovery stops the discovery service
func (cm *ClusterManagerImpl) StopDiscovery(ctx context.Context) error {
	if cm.discoveryManager != nil {
		return cm.discoveryManager.StopDiscovery(ctx)
	}
	return nil
}

// StopSwim stops the SWIM gossip protocol
func (cm *ClusterManagerImpl) StopSwim() {
	if cm.swimGossip != nil {
		cm.swimGossip.Stop()
	}
}

// JoinNode adds a new node to the cluster
func (cm *ClusterManagerImpl) JoinNode(ctx context.Context, node Node) error {
	if node.ID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	if node.Address == "" {
		return fmt.Errorf("node address cannot be empty")
	}

	// Check if node is reachable
	if err := cm.healthChecker.Check(ctx, node.Address); err != nil {
		return fmt.Errorf("node is not reachable: %v", err)
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check if node already exists
	if _, exists := cm.nodes[node.ID]; exists {
		return fmt.Errorf("node %s already exists", node.ID)
	}

	node.Status = NodeStatusActive
	cm.nodes[node.ID] = node

	// Add node to failure detector and gossip protocol
	cm.failureDetector.AddNode(node.ID, node.Address)
	cm.gossipProtocol.AddNode(node.ID, node.Address)

	// Broadcast membership change
	go cm.broadcastMembershipChange(node, true)

	return nil
}

// LeaveNode removes a node from the cluster
func (cm *ClusterManagerImpl) LeaveNode(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	node, exists := cm.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s does not exist", nodeID)
	}

	// Check if node has primary shards
	if shardMgr, ok := cm.shardManager.(interface{ HasPrimaryShardsForNode(string) bool }); ok {
		if shardMgr.HasPrimaryShardsForNode(nodeID) {
			return fmt.Errorf("cannot remove node %s: node has primary shards", nodeID)
		}
	} else if cm.shardManager.HasPrimaryShards() {
		// Fallback to old method if new one is not available
		return fmt.Errorf("cannot remove node %s: node has primary shards", nodeID)
	}

	node.Status = NodeStatusRemoved
	delete(cm.nodes, nodeID)

	// Remove node from failure detector and gossip protocol
	cm.failureDetector.RemoveNode(nodeID)
	cm.gossipProtocol.RemoveNode(nodeID)

	// Broadcast membership change
	go cm.broadcastMembershipChange(node, false)

	return nil
}

// GetNode retrieves information about a specific node
func (cm *ClusterManagerImpl) GetNode(ctx context.Context, nodeID string) (*Node, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	node, exists := cm.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s does not exist", nodeID)
	}

	return &node, nil
}

// ListNodes returns a list of all nodes in the cluster
func (cm *ClusterManagerImpl) ListNodes(ctx context.Context) ([]Node, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	nodes := make([]Node, 0, len(cm.nodes))
	for _, node := range cm.nodes {
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// HealthChecker defines methods for checking node health
type HealthChecker interface {
	Check(ctx context.Context, address string) error
}

// HTTPHealthChecker implements HealthChecker using HTTP
type HTTPHealthChecker struct {
	client *http.Client
}

// NewHTTPHealthChecker creates a new HTTPHealthChecker instance
func NewHTTPHealthChecker(client *http.Client) *HTTPHealthChecker {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPHealthChecker{client: client}
}

// Check performs a health check on the specified address
func (hc *HTTPHealthChecker) Check(ctx context.Context, address string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/health", address), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := hc.client.Do(req)
	if err != nil {
		return fmt.Errorf("health check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed: status code %d", resp.StatusCode)
	}

	return nil
}

// broadcastMembershipChange notifies other nodes about membership changes
func (cm *ClusterManagerImpl) broadcastMembershipChange(node Node, isJoin bool) {
	// Use gossip protocol to broadcast changes
	message := GossipMessage{
		SenderID:  cm.shardManager.GetLocalNodeID(),
		Timestamp: time.Now(),
		HealthStatus: map[string]NodeHealth{
			node.ID: {
				IsHealthy:     isJoin,
				LastHeartbeat: time.Now(),
			},
		},
	}
	cm.gossipProtocol.HandleGossipMessage(message)
}

// holdsPrimaryShards checks if a node holds any primary shards
func (cm *ClusterManagerImpl) holdsPrimaryShards(nodeID string) bool {
	// Get all keys and check their primary nodes
	// This is a simplified implementation
	for _, node := range cm.shardManager.GetAllNodes() {
		if node == nodeID {
			return true
		}
	}
	return false
}
