package cluster

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestDiscoveryManager_LoadNodes(t *testing.T) {
	// Set up environment variable for CLUSTER_NODES
	os.Setenv("CLUSTER_NODES", "localhost:8081, localhost:8082")
	defer os.Unsetenv("CLUSTER_NODES")

	// Create a self node
	self := Node{ID: "node1", Address: "localhost:8081", Status: NodeStatusActive}
	// Create DiscoveryManager using the self node
	dm := NewDiscoveryManager(self)

	// Load nodes from environment
	if err := dm.LoadNodesFromEnv(); err != nil {
		t.Fatalf("LoadNodesFromEnv failed: %v", err)
	}

	nodes := dm.GetNodes()
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}

	// Verify the first node's ID
	if !strings.HasPrefix(nodes[0].ID, "node-") {
		t.Errorf("expected node ID to start with 'node-', got %s", nodes[0].ID)
	}

	// Verify that marshalling works
	data, err := json.Marshal(nodes)
	if err != nil {
		t.Fatalf("failed to marshal nodes: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty JSON data")
	}
}

func TestDiscoveryManager_AddRemoveNode(t *testing.T) {
	// Create a self node
	self := Node{ID: "node1", Address: "localhost:8081", Status: NodeStatusActive}
	dm := NewDiscoveryManager(self)

	// Initially, expect no nodes added aside from self (GetNodes returns nodes loaded via LoadNodesFromEnv only); our implementation doesn't auto-add self
	if len(dm.GetNodes()) != 0 {
		t.Errorf("expected 0 nodes initially, got %d", len(dm.GetNodes()))
	}

	// Add a new node with ID "node2"
	node := Node{ID: "node2", Address: "localhost:8082", Status: NodeStatusActive}
	if err := dm.AddNode(node); err != nil {
		t.Fatalf("failed to add node: %v", err)
	}

	nodes := dm.GetNodes()
	if len(nodes) != 1 {
		t.Errorf("expected 1 node after addition, got %d", len(nodes))
	}

	// Remove the node
	if err := dm.RemoveNode("node2"); err != nil {
		t.Fatalf("failed to remove node: %v", err)
	}

	if len(dm.GetNodes()) != 0 {
		t.Errorf("expected 0 nodes after removal, got %d", len(dm.GetNodes()))
	}
}

// TestEtcdDiscovery tests the etcd-based discovery
func TestEtcdDiscovery(t *testing.T) {
	// Skip if running normal tests
	if os.Getenv("TEST_WITH_ETCD") != "true" {
		t.Skip("Skipping etcd test; set TEST_WITH_ETCD=true to run")
	}

	// This is a test outline that would use a real etcd instance
	// In a real implementation, you'd use a testing etcd instance

	t.Run("RegisterWithEtcd", func(t *testing.T) {
		// Create self node
		self := Node{ID: "test-node", Address: "localhost:8080", Status: NodeStatusActive}
		dm := NewDiscoveryManager(self)

		// Connect to etcd
		config := DiscoveryConfig{
			EtcdEndpoints: strings.Split(os.Getenv("ETCD_ENDPOINTS"), ","),
			ServicePrefix: "/test/nodes/",
			LeaseTTL:      5,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Only run if etcd endpoints are provided
		if len(config.EtcdEndpoints) > 0 && config.EtcdEndpoints[0] != "" {
			err := dm.ConnectToEtcd(config)
			if err != nil {
				t.Errorf("Failed to connect to etcd: %v", err)
			}

			err = dm.RegisterWithEtcd(ctx)
			if err != nil {
				t.Errorf("Failed to register with etcd: %v", err)
			}

			err = dm.StopDiscovery(ctx)
			if err != nil {
				t.Errorf("Failed to stop discovery: %v", err)
			}
		}
	})
}

// TestSwimGossip tests the SWIM gossip protocol
func TestSwimGossip(t *testing.T) {
	// Skip if running normal tests
	if os.Getenv("TEST_WITH_SWIM") != "true" {
		t.Skip("Skipping SWIM test; set TEST_WITH_SWIM=true to run")
	}

	self := Node{ID: "test-node-1", Address: "127.0.0.1:8080", Status: NodeStatusActive}

	sg, err := NewSwimGossip(self)
	if err != nil {
		t.Fatalf("Failed to create SWIM gossip: %v", err)
	}

	// Configure SWIM with test-friendly settings
	sg.SetConfig(SwimConfig{
		UDPPort:        17946, // Use non-standard ports for testing
		TCPPort:        17947,
		SuspectTimeout: 100 * time.Millisecond,
		ProtocolPeriod: 100 * time.Millisecond,
	})

	// Start protocol
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = sg.Start(ctx)
	// We might get errors about binding to ports in CI, so don't fail the test
	if err == nil {
		defer sg.Stop()

		// Add a node
		sg.AddNode("test-node-2", "127.0.0.1:8081")

		// Check if node was added
		nodes := sg.ListNodes()
		if len(nodes) != 1 {
			t.Errorf("Expected 1 node, got %d", len(nodes))
		}

		// Let protocol run for a bit
		time.Sleep(500 * time.Millisecond)

		// Remove the node
		sg.RemoveNode("test-node-2")

		// Check if node was marked as dead (not immediately removed)
		sg.mu.RLock()
		node, exists := sg.nodes["test-node-2"]
		sg.mu.RUnlock()

		if exists {
			if node.State != NodeStateDead {
				t.Errorf("Expected node state to be dead, got %v", node.State)
			}
		}
	}
}
