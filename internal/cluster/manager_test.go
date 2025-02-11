package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func setupTestClusterManager(t *testing.T) (ClusterManager, *mockShardManager) {
	shardMgr := newMockShardManager("local-node")
	fd := NewFailureDetector(100*time.Millisecond, 2)
	gp := NewGossipProtocol("local-node", fd)
	hc := NewHTTPHealthChecker(nil)
	cm := NewClusterManager(shardMgr, hc, fd, gp)
	return cm, shardMgr
}

func TestClusterManager_JoinNode(t *testing.T) {
	// Create test server for node health checks
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cm, _ := setupTestClusterManager(t)

	tests := []struct {
		name    string
		node    Node
		wantErr bool
	}{
		{
			name: "Valid node",
			node: Node{
				ID:      "test-node-1",
				Address: server.URL[7:], // Strip "http://"
			},
			wantErr: false,
		},
		{
			name: "Empty node ID",
			node: Node{
				ID:      "",
				Address: server.URL[7:],
			},
			wantErr: true,
		},
		{
			name: "Empty address",
			node: Node{
				ID:      "test-node-2",
				Address: "",
			},
			wantErr: true,
		},
		{
			name: "Duplicate node ID",
			node: Node{
				ID:      "test-node-1",
				Address: server.URL[7:],
			},
			wantErr: true,
		},
		{
			name: "Unreachable node",
			node: Node{
				ID:      "test-node-3",
				Address: "invalid-host:1234",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cm.JoinNode(context.Background(), tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("JoinNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClusterManager_LeaveNode(t *testing.T) {
	cm, shardMgr := setupTestClusterManager(t)

	// Add test nodes
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	node1 := Node{
		ID:      "test-node-1",
		Address: server.URL[7:],
	}
	node2 := Node{
		ID:      "test-node-2",
		Address: server.URL[7:],
	}

	// Add nodes to cluster
	if err := cm.JoinNode(context.Background(), node1); err != nil {
		t.Fatalf("Failed to add node1: %v", err)
	}
	if err := cm.JoinNode(context.Background(), node2); err != nil {
		t.Fatalf("Failed to add node2: %v", err)
	}

	tests := []struct {
		name    string
		nodeID  string
		setup   func()
		wantErr bool
	}{
		{
			name:    "Remove existing node",
			nodeID:  "test-node-1",
			setup:   func() {},
			wantErr: false,
		},
		{
			name:    "Remove non-existent node",
			nodeID:  "non-existent",
			setup:   func() {},
			wantErr: true,
		},
		{
			name:   "Remove node with primary shards",
			nodeID: "test-node-2",
			setup: func() {
				// Setup node to hold primary shards
				shardMgr.responsibleNodes["test-key"] = []string{"test-node-2"}
			},
			wantErr: true,
		},
		{
			name:    "Remove already removed node",
			nodeID:  "test-node-1",
			setup:   func() {},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			err := cm.LeaveNode(context.Background(), tt.nodeID)
			if (err != nil) != tt.wantErr {
				t.Errorf("LeaveNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClusterManager_GetNode(t *testing.T) {
	cm, _ := setupTestClusterManager(t)
	ctx := context.Background()

	// Add test node
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	node := Node{
		ID:      "test-node",
		Address: server.URL[7:],
	}

	if err := cm.JoinNode(ctx, node); err != nil {
		t.Fatalf("Failed to add node: %v", err)
	}

	tests := []struct {
		name    string
		nodeID  string
		wantErr bool
	}{
		{
			name:    "Get existing node",
			nodeID:  "test-node",
			wantErr: false,
		},
		{
			name:    "Get non-existent node",
			nodeID:  "non-existent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := cm.GetNode(ctx, tt.nodeID)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetNode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && node.ID != tt.nodeID {
				t.Errorf("GetNode() got = %v, want %v", node.ID, tt.nodeID)
			}
		})
	}
}

func TestClusterManager_ListNodes(t *testing.T) {
	cm, _ := setupTestClusterManager(t)
	ctx := context.Background()

	// Add test nodes
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	nodes := []Node{
		{ID: "node1", Address: server.URL[7:]},
		{ID: "node2", Address: server.URL[7:]},
		{ID: "node3", Address: server.URL[7:]},
	}

	// Add nodes to cluster
	for _, node := range nodes {
		if err := cm.JoinNode(ctx, node); err != nil {
			t.Fatalf("Failed to add node %s: %v", node.ID, err)
		}
	}

	// Remove one node
	if err := cm.LeaveNode(ctx, "node2"); err != nil {
		t.Fatalf("Failed to remove node: %v", err)
	}

	// List nodes
	activeNodes, err := cm.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes failed: %v", err)
	}

	// Should have 2 active nodes
	if len(activeNodes) != 2 {
		t.Errorf("ListNodes() got %d nodes, want 2", len(activeNodes))
	}

	// Verify active nodes
	nodeMap := make(map[string]bool)
	for _, node := range activeNodes {
		nodeMap[node.ID] = true
	}

	if !nodeMap["node1"] || !nodeMap["node3"] {
		t.Error("ListNodes() missing expected active nodes")
	}
	if nodeMap["node2"] {
		t.Error("ListNodes() includes removed node")
	}
}
