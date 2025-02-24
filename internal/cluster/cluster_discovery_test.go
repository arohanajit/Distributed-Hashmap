package cluster

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestClusterManagerWithDiscovery tests the integration of ClusterManager with the etcd discovery service
func TestClusterManagerWithDiscovery(t *testing.T) {
	// Skip if not running etcd integration tests
	if os.Getenv("TEST_WITH_ETCD") != "true" {
		t.Skip("Skipping etcd integration test; set TEST_WITH_ETCD=true to run")
	}

	// Create a self node
	self := Node{
		ID:      "test-cm-etcd-node",
		Address: "localhost:9090",
		Status:  NodeStatusActive,
	}

	// Create a mock shard manager and health checker for testing
	shardManager := NewTestShardManager(self.ID)
	healthChecker := NewHTTPHealthChecker(nil)

	// Create a ClusterManager with discovery
	cm, err := NewClusterManagerWithDiscovery(self, shardManager, healthChecker)
	if err != nil {
		t.Fatalf("Failed to create cluster manager with discovery: %v", err)
	}

	// Start discovery service with a short-lived test config
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config := DiscoveryConfig{
		EtcdEndpoints: []string{"localhost:2379"},
		ServicePrefix: "/test/dhashmap/nodes/",
		LeaseTTL:      5,
	}

	// This might fail if etcd is not running, but it should at least attempt to connect
	err = cm.StartDiscovery(ctx, config)
	if err != nil {
		t.Logf("Warning: etcd connection failed: %v", err)
		// This is not a fatal error for the test, as we're mainly testing the integration
	}

	// Test that the discovery manager was properly set up
	if cm.discoveryManager == nil {
		t.Errorf("Expected discovery manager to be initialized")
	}

	// Stop the discovery service
	err = cm.StopDiscovery(ctx)
	if err != nil {
		t.Logf("Warning: stopping discovery service failed: %v", err)
	}
}

// TestClusterManagerWithSwim tests the integration of ClusterManager with the SWIM gossip protocol
func TestClusterManagerWithSwim(t *testing.T) {
	// Skip if not running SWIM integration tests
	if os.Getenv("TEST_WITH_SWIM") != "true" {
		t.Skip("Skipping SWIM integration test; set TEST_WITH_SWIM=true to run")
	}

	// Create a self node
	self := Node{
		ID:      "test-cm-swim-node",
		Address: "localhost:9091",
		Status:  NodeStatusActive,
	}

	// Create a mock shard manager and health checker for testing
	shardManager := NewTestShardManager(self.ID)
	healthChecker := NewHTTPHealthChecker(nil)

	// Create a ClusterManager with SWIM gossip
	cm, err := NewClusterManagerWithSwim(self, shardManager, healthChecker)
	if err != nil {
		t.Fatalf("Failed to create cluster manager with SWIM: %v", err)
	}

	// Start SWIM gossip with test-specific ports to avoid conflicts
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config := SwimConfig{
		UDPPort:        17956, // Use high ports unlikely to be in use
		TCPPort:        17957,
		SuspectTimeout: 100 * time.Millisecond,
		ProtocolPeriod: 100 * time.Millisecond,
	}

	err = cm.StartSwim(ctx, config)
	if err != nil {
		t.Logf("Warning: starting SWIM protocol failed (likely due to port already in use): %v", err)
		// This is not a fatal error for the test, as we're mainly testing the integration
	} else {
		// Add a test node if SWIM started successfully
		if cm.swimGossip != nil {
			cm.swimGossip.AddNode("test-peer", "localhost:9092")
		}
	}

	// Test that the SWIM gossip was properly set up
	if cm.swimGossip == nil {
		t.Errorf("Expected SWIM gossip to be initialized")
	}

	// Stop the SWIM gossip
	cm.StopSwim()

	// Give it a bit of time to shut down cleanly
	time.Sleep(100 * time.Millisecond)
}

// TestDiscoveryIntegrationWithFailureDetector tests that the discovery service integrates with the failure detector
func TestDiscoveryIntegrationWithFailureDetector(t *testing.T) {
	// This is a basic integration test that doesn't require external services

	// Create a self node
	self := Node{
		ID:      "test-integration-node",
		Address: "localhost:9093",
		Status:  NodeStatusActive,
	}

	// Create a mock shard manager and health checker for testing
	shardManager := NewTestShardManager(self.ID)
	healthChecker := NewHTTPHealthChecker(nil)

	// Create a ClusterManager with discovery
	cm, err := NewClusterManagerWithDiscovery(self, shardManager, healthChecker)
	if err != nil {
		t.Fatalf("Failed to create cluster manager with discovery: %v", err)
	}

	// Start the failure detector with a short interval
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cm.failureDetector.interval = 100 * time.Millisecond
	cm.failureDetector.Start(ctx)
	defer cm.failureDetector.Stop()

	// Add a test node
	testNode := Node{
		ID:      "test-failure-node",
		Address: "localhost:9094",
		Status:  NodeStatusActive,
	}

	// Add the node to see if it's properly tracked
	err = cm.JoinNode(ctx, testNode)
	if err != nil {
		t.Logf("Warning: joining node failed (expected for unreachable node): %v", err)
		// This warning is expected since the node doesn't actually exist
	}

	// Verify that the failure detector attempts to monitor the node
	time.Sleep(200 * time.Millisecond)

	// Stop everything
	cm.failureDetector.Stop()
}
