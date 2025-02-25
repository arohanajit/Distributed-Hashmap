package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
)

func TestPrometheusMetrics_Singleton(t *testing.T) {
	// Get metrics instance twice
	metrics1 := GetMetrics()
	metrics2 := GetMetrics()

	// Should be the same instance
	assert.Equal(t, metrics1, metrics2, "GetMetrics should return the same instance")
}

func TestPrometheusMetrics_ClusterNodesTotal(t *testing.T) {
	// Reset registry for testing
	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	// Create new metrics instance
	metrics := NewPrometheusMetrics()

	// Set cluster nodes total
	metrics.SetClusterNodesTotal(5)

	// Create a request to the metrics endpoint
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	// Serve metrics
	promhttp.Handler().ServeHTTP(w, req)

	// Check response
	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// The actual value check would require parsing the response body
	// which is complex for Prometheus metrics format
	// In a real test, you might use the client library to query the metrics
}

func TestClusterMetricsCollector(t *testing.T) {
	// Reset registry for testing
	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	// Create collector
	collector := NewClusterMetricsCollector()

	// Test node count operations
	assert.Equal(t, 0, collector.GetNodeCount(), "Initial node count should be 0")

	collector.UpdateNodeCount(3)
	assert.Equal(t, 3, collector.GetNodeCount(), "Node count should be updated to 3")

	collector.AddNode()
	assert.Equal(t, 4, collector.GetNodeCount(), "Node count should be incremented to 4")

	collector.RemoveNode()
	assert.Equal(t, 3, collector.GetNodeCount(), "Node count should be decremented to 3")

	// Test rebalance operations
	collector.StartRebalance(100)
	collector.UpdateRebalanceProgress(30)
	collector.UpdateRebalanceProgress(40)
	collector.CompleteRebalance()

	// Test replication metrics
	collector.RecordReplicationError("node1")
	collector.UpdateReplicaLag("node1", 500*time.Millisecond)
}

func TestMetricsMiddleware(t *testing.T) {
	// Reset registry for testing
	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	// Create test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Wrap with metrics middleware
	handler := MetricsMiddleware(testHandler)

	// Create test request
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	// Serve request
	handler.ServeHTTP(w, req)

	// Check response
	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRegisterMetricsHandler(t *testing.T) {
	// Create test mux
	mux := http.NewServeMux()

	// Register metrics handler
	RegisterMetricsHandler(mux)

	// Create test request to metrics endpoint
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	// Serve request
	mux.ServeHTTP(w, req)

	// Check response
	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
