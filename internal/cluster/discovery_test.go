package cluster

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
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

	// Optionally, verify the first node's ID
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
