package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/arohanajit/Distributed-Hashmap/internal/storage"
)

const (
	maxPayloadSize = 5 * 1024 * 1024 // 5MB
)

// Handler handles HTTP requests for key-value operations
type Handler struct {
	store storage.Store
}

// KeyHandler handles operations on keys in the store
type KeyHandler struct {
	store storage.Store
}

// NewKeyHandler creates a new instance of KeyHandler
func NewKeyHandler(store storage.Store) *KeyHandler {
	return &KeyHandler{store: store}
}

// NewHandler creates a new instance of Handler
func NewHandler(store storage.Store) *Handler {
	return &Handler{
		store: store,
	}
}

// handleGet handles GET requests for key retrieval
func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	data, contentType, err := h.store.Get(key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Write(data)
}

// handlePut handles PUT requests for key-value storage
func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.store.Put(key, data, contentType); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// handleDelete handles DELETE requests for key removal
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	if err := h.store.Delete(key); err != nil {
		if err == storage.ErrKeyNotFound {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// PutHandler handles PUT requests to store data with content type preservation
func (h *Handler) PutHandler(w http.ResponseWriter, r *http.Request) {
	// Extract the key from the URL query
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	// Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Get content type from header
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {

		contentType = "application/octet-stream"
	}

	// Store the item using the new Put signature (key, data, contentType)
	if err := h.store.Put(key, body, contentType); err != nil {
		http.Error(w, fmt.Sprintf("failed to store value: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// GetHandler handles GET requests and returns data with original content type
func (h *Handler) GetHandler(w http.ResponseWriter, r *http.Request) {
	// Extract the key from the URL query
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	// Retrieve the item using the new Get signature
	data, contentType, err := h.store.Get(key)

	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get value: %v", err), http.StatusNotFound)
		return
	}

	// Write response with original content type
	w.Header().Set("Content-Type", contentType)
	w.Write(data)
}

// Put handles PUT requests to store a value
func (h *KeyHandler) Put(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	// Validate key
	if key == "" {
		http.Error(w, "Key cannot be empty", http.StatusBadRequest)
		return
	}

	// Check Content-Length
	if r.ContentLength > maxPayloadSize {
		http.Error(w, fmt.Sprintf("Payload too large. Maximum size is %d bytes", maxPayloadSize), http.StatusRequestEntityTooLarge)
		return
	}

	// Read body with size limit
	body, err := io.ReadAll(io.LimitReader(r.Body, maxPayloadSize))
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// Get content type from header
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Validate JSON if content type is application/json
	if strings.HasPrefix(contentType, "application/json") {
		if !json.Valid(body) {
			http.Error(w, "Invalid JSON format", http.StatusBadRequest)
			return
		}
	}

	// Store the value using updated Put signature
	if err := h.store.Put(key, body, contentType); err != nil {
		switch err {
		case storage.ErrEmptyKey:
			http.Error(w, err.Error(), http.StatusBadRequest)
		case storage.ErrNilValue:
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}

// Get handles GET requests to retrieve a value
func (h *KeyHandler) Get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	// Validate key
	if key == "" {
		http.Error(w, "Key cannot be empty", http.StatusBadRequest)
		return
	}

	// Retrieve the value using updated Get signature
	data, contentType, err := h.store.Get(key)
	if err != nil {
		switch err {
		case storage.ErrKeyNotFound:
			http.Error(w, "Key not found", http.StatusNotFound)
		case storage.ErrEmptyKey:
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Write(data)
}

// Delete handles DELETE requests to remove a value
func (h *KeyHandler) Delete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	// Validate key
	if key == "" {
		http.Error(w, "Key cannot be empty", http.StatusBadRequest)
		return
	}

	if err := h.store.Delete(key); err != nil {
		switch err {
		case storage.ErrKeyNotFound:
			http.Error(w, "Key not found", http.StatusNotFound)
		case storage.ErrEmptyKey:
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// LoggingMiddleware logs request details
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a custom response writer to capture status code
		rw := &responseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		// Process request
		next.ServeHTTP(rw, r)

		// Log request details
		fmt.Printf("[%s] %s %s - %d\n",
			r.Method,
			r.URL.Path,
			r.RemoteAddr,
			rw.statusCode,
		)
	})
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// TimeoutMiddleware adds a timeout to the request context
func TimeoutMiddleware(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()

			r = r.WithContext(ctx)
			done := make(chan struct{})

			go func() {
				next.ServeHTTP(w, r)
				close(done)
			}()

			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					http.Error(w, "Request timeout", http.StatusGatewayTimeout)
				}
			case <-done:
				// Request completed before timeout
			}
		})
	}
}
