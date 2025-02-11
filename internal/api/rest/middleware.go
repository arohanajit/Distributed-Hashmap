package rest

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/arohanajit/Distributed-Hashmap/internal/cluster"
)

const (
	maxRedirects   = 5
	requestTimeout = 10 * time.Second
	// keyHeader is the header containing the key for the request
	keyHeader = "X-Key"
)

// ShardAwareMiddleware is middleware that checks if the current node is responsible for the requested key
type ShardAwareMiddleware struct {
	shardManager cluster.ShardManager
}

// NewShardAwareMiddleware creates a new instance of ShardAwareMiddleware
func NewShardAwareMiddleware(sm cluster.ShardManager) *ShardAwareMiddleware {
	return &ShardAwareMiddleware{
		shardManager: sm,
	}
}

// ShardLookupMiddleware handles routing requests to the appropriate node based on key sharding
type ShardLookupMiddleware struct {
	shardManager cluster.ShardManager
	nodeID       string
	client       *http.Client
}

// NewShardLookupMiddleware creates a new instance of ShardLookupMiddleware
func NewShardLookupMiddleware(shardManager cluster.ShardManager, nodeID string) *ShardLookupMiddleware {
	return &ShardLookupMiddleware{
		shardManager: shardManager,
		nodeID:       nodeID,
		client: &http.Client{
			Timeout: requestTimeout,
		},
	}
}

// Middleware implements the http.Handler interface
func (m *ShardLookupMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract key from request
		key := r.Header.Get(keyHeader)
		if key == "" {
			key = strings.TrimPrefix(r.URL.Path, "/keys/")
		}

		if key == "" {
			http.Error(w, "key not found in request", http.StatusBadRequest)
			return
		}

		// Check if this node is responsible for the key
		responsibleNode := m.shardManager.GetNodeForKey(key)
		if responsibleNode == m.nodeID {
			next.ServeHTTP(w, r)
			return
		}

		// Get node address and redirect the request
		nodeAddr := m.shardManager.GetNodeAddress(responsibleNode)
		if nodeAddr == "" {
			http.Error(w, fmt.Sprintf("no address found for node %s", responsibleNode), http.StatusInternalServerError)
			return
		}

		// Build target URL and redirect
		targetURL := fmt.Sprintf("http://%s%s", nodeAddr, r.URL.Path)
		http.Redirect(w, r, targetURL, http.StatusTemporaryRedirect)
	})
}

// ProxyRequest proxies the request to the target node instead of redirecting
// This is an alternative to redirection if needed
func (m *ShardLookupMiddleware) proxyRequest(targetURL string, w http.ResponseWriter, r *http.Request) {
	// Create new request
	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create proxy request: %v", err), http.StatusInternalServerError)
		return
	}

	// Copy headers
	for key, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	// Forward the request
	resp, err := m.client.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to proxy request: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Copy status code and body
	w.WriteHeader(resp.StatusCode)
	limitedReader := io.LimitReader(resp.Body, 1<<20) // 1MB limit
	if _, err := io.Copy(w, limitedReader); err != nil {
		// Log error but don't send to client as headers are already sent
		fmt.Printf("Error copying response body: %v\n", err)
	}
}
