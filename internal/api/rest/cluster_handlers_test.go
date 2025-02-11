package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/arohanajit/Distributed-Hashmap/internal/cluster"
	"github.com/gorilla/mux"
)

// mockClusterManager implements cluster.ClusterManager interface for testing
type mockClusterManager struct {
	nodes map[string]*cluster.Node
}

func newMockClusterManager() cluster.ClusterManager {
	return &mockClusterManager{
		nodes: make(map[string]*cluster.Node),
	}
}

func (m *mockClusterManager) JoinNode(ctx context.Context, node cluster.Node) error {
	if node.ID == "" || node.Address == "" {
		return fmt.Errorf("invalid node: ID and address are required")
	}
	if _, exists := m.nodes[node.ID]; exists {
		return fmt.Errorf("node %s already exists", node.ID)
	}
	m.nodes[node.ID] = &node
	return nil
}

func (m *mockClusterManager) LeaveNode(ctx context.Context, nodeID string) error {
	if _, exists := m.nodes[nodeID]; !exists {
		return fmt.Errorf("node not found")
	}
	delete(m.nodes, nodeID)
	return nil
}

func (m *mockClusterManager) GetNode(ctx context.Context, nodeID string) (*cluster.Node, error) {
	node, exists := m.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node not found")
	}
	return node, nil
}

func (m *mockClusterManager) ListNodes(ctx context.Context) ([]cluster.Node, error) {
	nodes := make([]cluster.Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, *node)
	}
	return nodes, nil
}

func setupClusterHandlerTest(t *testing.T) (*ClusterHandler, cluster.ClusterManager, *mux.Router) {
	cm := newMockClusterManager()
	handler := &ClusterHandler{clusterManager: cm}
	router := mux.NewRouter()
	handler.RegisterRoutes(router)
	return handler, cm, router
}

func TestClusterHandler_AddNode(t *testing.T) {
	_, _, router := setupClusterHandlerTest(t)

	tests := []struct {
		name       string
		payload    map[string]string
		wantStatus int
	}{
		{
			name: "Valid node",
			payload: map[string]string{
				"id":      "test-node-1",
				"address": "localhost:8081",
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "Empty node ID",
			payload: map[string]string{
				"id":      "",
				"address": "localhost:8081",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "Empty address",
			payload: map[string]string{
				"id":      "test-node-2",
				"address": "",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "Invalid JSON",
			payload: map[string]string{
				"invalid": "json",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.payload)
			req := httptest.NewRequest(http.MethodPost, "/cluster/nodes", bytes.NewReader(body))
			rr := httptest.NewRecorder()

			router.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Errorf("handler returned wrong status code: got %v want %v",
					rr.Code, tt.wantStatus)
			}
		})
	}
}

func TestClusterHandler_RemoveNode(t *testing.T) {
	_, cm, router := setupClusterHandlerTest(t)

	// Add a test node
	node := cluster.Node{
		ID:      "test-node",
		Address: "localhost:8081",
	}
	cm.JoinNode(context.Background(), node)

	tests := []struct {
		name       string
		nodeID     string
		wantStatus int
	}{
		{
			name:       "Remove existing node",
			nodeID:     "test-node",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "Remove non-existent node",
			nodeID:     "non-existent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodDelete, "/cluster/nodes/"+tt.nodeID, nil)
			rr := httptest.NewRecorder()

			router.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Errorf("handler returned wrong status code: got %v want %v",
					rr.Code, tt.wantStatus)
			}
		})
	}
}

func TestClusterHandler_ListNodes(t *testing.T) {
	_, cm, router := setupClusterHandlerTest(t)

	// Add test nodes
	nodes := []cluster.Node{
		{ID: "node1", Address: "localhost:8081"},
		{ID: "node2", Address: "localhost:8082"},
	}

	for _, node := range nodes {
		cm.JoinNode(context.Background(), node)
	}

	req := httptest.NewRequest(http.MethodGet, "/cluster/nodes", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			rr.Code, http.StatusOK)
	}

	var response []cluster.Node
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(response) != len(nodes) {
		t.Errorf("handler returned wrong number of nodes: got %v want %v",
			len(response), len(nodes))
	}
}

func TestClusterHandler_GetNode(t *testing.T) {
	_, cm, router := setupClusterHandlerTest(t)

	// Add a test node
	node := cluster.Node{
		ID:      "test-node",
		Address: "localhost:8081",
	}
	cm.JoinNode(context.Background(), node)

	tests := []struct {
		name       string
		nodeID     string
		wantStatus int
	}{
		{
			name:       "Get existing node",
			nodeID:     "test-node",
			wantStatus: http.StatusOK,
		},
		{
			name:       "Get non-existent node",
			nodeID:     "non-existent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/cluster/nodes/"+tt.nodeID, nil)
			rr := httptest.NewRecorder()

			router.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Errorf("handler returned wrong status code: got %v want %v",
					rr.Code, tt.wantStatus)
			}

			if tt.wantStatus == http.StatusOK {
				var response cluster.Node
				if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
					t.Fatalf("Failed to decode response: %v", err)
				}

				if response.ID != tt.nodeID {
					t.Errorf("handler returned wrong node: got %v want %v",
						response.ID, tt.nodeID)
				}
			}
		})
	}
}
