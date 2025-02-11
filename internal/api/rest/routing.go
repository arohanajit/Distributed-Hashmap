package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
)

// Router handles HTTP routing for the key-value store
type Router struct {
	store Store
}

// Store interface defines the contract for storage operations
type Store interface {
	Get(key string) ([]byte, string, error)
	Put(key string, data []byte, contentType string) error
	Delete(key string) error
}

// NewRouter creates a new instance of Router
func NewRouter(store Store) *Router {
	return &Router{
		store: store,
	}
}

// RegisterRoutes registers all HTTP routes with their handlers
func (r *Router) RegisterRoutes(mux *http.ServeMux, middleware func(http.Handler) http.Handler) {
	// Apply middleware to all routes
	mux.Handle("/keys/", middleware(http.HandlerFunc(r.handleKeyOperations)))
	mux.Handle("/health", http.HandlerFunc(r.handleHealth))
}

// handleKeyOperations handles all operations on keys (GET, PUT, DELETE)
func (r *Router) handleKeyOperations(w http.ResponseWriter, req *http.Request) {
	// Extract key from URL path
	key := path.Base(req.URL.Path)
	if key == "" || key == "keys" {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}

	switch req.Method {
	case http.MethodGet:
		r.handleGet(w, req, key)
	case http.MethodPut:
		r.handlePut(w, req, key)
	case http.MethodDelete:
		r.handleDelete(w, req, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGet handles GET requests for a key
func (r *Router) handleGet(w http.ResponseWriter, req *http.Request, key string) {
	data, contentType, err := r.store.Get(key)
	if err != nil {
		if err.Error() == "key not found" {
			http.Error(w, fmt.Sprintf("key not found: %v", err), http.StatusNotFound)
		} else {
			http.Error(w, fmt.Sprintf("failed to get key: %v", err), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// handlePut handles PUT requests for a key
func (r *Router) handlePut(w http.ResponseWriter, req *http.Request, key string) {
	// Read request body
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusBadRequest)
		return
	}
	defer req.Body.Close()

	// Get content type, default to application/octet-stream
	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Store the data
	err = r.store.Put(key, body, contentType)
	if err != nil {
		if err.Error() == "simulated error" {
			http.Error(w, fmt.Sprintf("failed to store key: %v", err), http.StatusInternalServerError)
		} else {
			http.Error(w, fmt.Sprintf("failed to store key: %v", err), http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// handleDelete handles DELETE requests for a key
func (r *Router) handleDelete(w http.ResponseWriter, req *http.Request, key string) {
	err := r.store.Delete(key)
	if err != nil {
		if err.Error() == "key not found" {
			http.Error(w, fmt.Sprintf("key not found: %v", err), http.StatusNotFound)
		} else {
			http.Error(w, fmt.Sprintf("failed to delete key: %v", err), http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleHealth handles health check requests
func (r *Router) handleHealth(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	response := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
