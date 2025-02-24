package cluster

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Example showing how to use the etcd-based discovery service
func RunEtcdDiscoveryExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a node representing this instance
	self := Node{
		ID:      fmt.Sprintf("node-%s", os.Getenv("NODE_ID")),
		Address: fmt.Sprintf("%s:%s", os.Getenv("NODE_HOST"), os.Getenv("NODE_PORT")),
		Status:  NodeStatusActive,
	}

	// Create shard manager and health checker
	shardManager := NewTestShardManager(self.ID)
	healthChecker := NewHTTPHealthChecker(nil)

	// Create cluster manager with etcd discovery
	cm, err := NewClusterManagerWithDiscovery(self, shardManager, healthChecker)
	if err != nil {
		log.Fatalf("Failed to create cluster manager: %v", err)
	}

	// Start failure detector
	cm.failureDetector.Start(ctx)
	defer cm.failureDetector.Stop()

	// Start discovery service
	etcdConfig := DiscoveryConfig{
		EtcdEndpoints: []string{os.Getenv("ETCD_ENDPOINTS")},
		ServicePrefix: "/services/hashmap/nodes/",
		LeaseTTL:      30, // 30 seconds
	}

	if err := cm.StartDiscovery(ctx, etcdConfig); err != nil {
		log.Fatalf("Failed to start discovery service: %v", err)
	}
	defer cm.StopDiscovery(ctx)

	// Wait for signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
}

// Example showing how to use the SWIM gossip protocol
func RunSwimGossipExample() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a node representing this instance
	self := Node{
		ID:      fmt.Sprintf("node-%s", os.Getenv("NODE_ID")),
		Address: fmt.Sprintf("%s:%s", os.Getenv("NODE_HOST"), os.Getenv("NODE_PORT")),
		Status:  NodeStatusActive,
	}

	// Create shard manager and health checker
	shardManager := NewTestShardManager(self.ID)
	healthChecker := NewHTTPHealthChecker(nil)

	// Create cluster manager with SWIM gossip
	cm, err := NewClusterManagerWithSwim(self, shardManager, healthChecker)
	if err != nil {
		log.Fatalf("Failed to create cluster manager: %v", err)
	}

	// Start failure detector
	cm.failureDetector.Start(ctx)
	defer cm.failureDetector.Stop()

	// Start SWIM gossip protocol
	swimConfig := SwimConfig{
		UDPPort:        7946,
		TCPPort:        7947,
		SuspectTimeout: 5 * time.Second,
		ProtocolPeriod: 1 * time.Second,
		IndirectNodes:  3,
		MaxBroadcasts:  3,
	}

	if err := cm.StartSwim(ctx, swimConfig); err != nil {
		log.Fatalf("Failed to start SWIM gossip: %v", err)
	}
	defer cm.StopSwim()

	// Check if there are seed nodes to join
	seedNodesEnv := os.Getenv("SEED_NODES")
	if seedNodesEnv != "" {
		// Parse and join seed nodes (implementation depends on your app)
		// This is just an example:
		seedNode := Node{
			ID:      "seed-1",
			Address: seedNodesEnv,
			Status:  NodeStatusActive,
		}

		// Request a full state sync from the seed node
		if err := cm.swimGossip.RequestSync(seedNode); err != nil {
			log.Printf("Warning: Failed to sync with seed node: %v", err)
		}
	}

	// Wait for signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
}

// NewTestShardManager creates a test shard manager for examples
func NewTestShardManager(nodeID string) ShardManager {
	return &TestShardManager{nodeID: nodeID}
}

// TestShardManager is a simple implementation of ShardManager for examples
type TestShardManager struct {
	nodeID string
}

func (sm *TestShardManager) GetLocalNodeID() string {
	return sm.nodeID
}

func (sm *TestShardManager) GetCurrentNodeID() string {
	return sm.nodeID
}

func (sm *TestShardManager) GetNodeAddress(nodeID string) string {
	return "localhost:8080" // Default for testing
}

func (sm *TestShardManager) GetAllNodes() []string {
	return []string{sm.nodeID}
}

func (sm *TestShardManager) HasPrimaryShards() bool {
	return false
}

func (sm *TestShardManager) HasPrimaryShardsForNode(nodeID string) bool {
	return false
}

func (sm *TestShardManager) GetNodeForKey(key string) string {
	return sm.nodeID // Always return self for testing
}

func (sm *TestShardManager) GetResponsibleNodes(key string) []string {
	return []string{sm.nodeID} // Always return self for testing
}

func (sm *TestShardManager) UpdateResponsibleNodes(key string, nodes []string) {
	// No-op for testing
}

func (sm *TestShardManager) GetSuccessors(nodeID string) []string {
	return []string{} // No successors in test
}

func (sm *TestShardManager) GetPredecessors(nodeID string) []string {
	return []string{} // No predecessors in test
}

func (sm *TestShardManager) GetSuccessorNodes(nodeID string, count int) []string {
	return []string{} // No successors in test
}
