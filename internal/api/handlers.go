package api

import (
	"io"
	"net/http"

	"github.com/arohanajit/Distributed-Hashmap/internal/storage"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

// Handler handles HTTP requests for key-value operations
type Handler struct {
	store  storage.Store
	logger *zap.Logger
}

// NewHandler creates a new instance of Handler
func NewHandler(store storage.Store, logger *zap.Logger) *Handler {
	return &Handler{
		store:  store,
		logger: logger,
	}
}

func (h *Handler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/v1/kv/{key}", h.HandleGet).Methods(http.MethodGet)
	r.HandleFunc("/v1/kv/{key}", h.HandlePut).Methods(http.MethodPut)
	r.HandleFunc("/v1/kv/{key}", h.HandleDelete).Methods(http.MethodDelete)
}

func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	data, contentType, err := h.store.Get(key)
	if err == storage.ErrKeyNotFound {
		h.logger.Debug("key not found", zap.String("key", key))
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	if err != nil {
		h.logger.Error("failed to get key", zap.String("key", key), zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Write(data)
}

func (h *Handler) HandlePut(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	data, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error("failed to read request body", zap.Error(err))
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if err := h.store.Put(key, data, contentType); err != nil {
		h.logger.Error("failed to put key", zap.String("key", key), zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := h.store.Delete(key)
	if err == storage.ErrKeyNotFound {
		h.logger.Debug("key not found for deletion", zap.String("key", key))
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	if err != nil {
		h.logger.Error("failed to delete key", zap.String("key", key), zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// Get retrieves a value for a given key
func (h *Handler) Get(key string) ([]byte, string, error) {
	return h.store.Get(key)
}

// Put stores a value with its content type for a given key
func (h *Handler) Put(key string, data []byte, contentType string) error {
	return h.store.Put(key, data, contentType)
}

// Delete removes a key-value pair
func (h *Handler) Delete(key string) error {
	return h.store.Delete(key)
}
