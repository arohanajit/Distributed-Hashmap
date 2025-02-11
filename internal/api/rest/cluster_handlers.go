package rest

import (
	"encoding/json"
	"net/http"

	"github.com/arohanajit/Distributed-Hashmap/internal/cluster"
	"github.com/gorilla/mux"
)

// ClusterHandler handles cluster management API endpoints
type ClusterHandler struct {
	clusterManager cluster.ClusterManager
}

// NewClusterHandler creates a new instance of ClusterHandler
func NewClusterHandler(cm cluster.ClusterManager) *ClusterHandler {
	return &ClusterHandler{
		clusterManager: cm,
	}
}

// RegisterRoutes registers cluster management routes
func (h *ClusterHandler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/cluster/nodes", h.handleAddNode).Methods(http.MethodPost)
	r.HandleFunc("/cluster/nodes/{nodeID}", h.handleRemoveNode).Methods(http.MethodDelete)
	r.HandleFunc("/cluster/nodes", h.handleListNodes).Methods(http.MethodGet)
	r.HandleFunc("/cluster/nodes/{nodeID}", h.handleGetNode).Methods(http.MethodGet)
}

// handleAddNode handles POST /cluster/nodes requests
func (h *ClusterHandler) handleAddNode(w http.ResponseWriter, r *http.Request) {
	var node cluster.Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.clusterManager.JoinNode(r.Context(), node); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// handleRemoveNode handles DELETE /cluster/nodes/{nodeID} requests
func (h *ClusterHandler) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeID"]

	if err := h.clusterManager.LeaveNode(r.Context(), nodeID); err != nil {
		if err.Error() == "node not found" {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleListNodes handles GET /cluster/nodes requests
func (h *ClusterHandler) handleListNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := h.clusterManager.ListNodes(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}

// handleGetNode handles GET /cluster/nodes/{nodeID} requests
func (h *ClusterHandler) handleGetNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeID"]

	node, err := h.clusterManager.GetNode(r.Context(), nodeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(node)
}
