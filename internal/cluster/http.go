package cluster

import (
	"encoding/json"
	"net/http"
)

// HTTPHandler handles HTTP requests for cluster operations
// It holds a ClusterManager (note: not a pointer to an interface)
type HTTPHandler struct {
	manager ClusterManager
}

// NewHTTPHandler creates a new HTTPHandler instance
func NewHTTPHandler(manager ClusterManager) *HTTPHandler {
	return &HTTPHandler{manager: manager}
}

// ServeHTTP handles HTTP requests; for example, listing nodes
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// For demonstration, assume this handler returns the list of nodes in JSON
	nodes, err := h.manager.ListNodes(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := json.Marshal(nodes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
