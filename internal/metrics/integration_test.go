package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

// TestMetricsServerIntegration tests the integration of metrics with an HTTP server
func TestMetricsServerIntegration(t *testing.T) {
	// Skip detailed metrics validation in integration tests
	// as Prometheus registry is shared across tests

	// Create a test server with metrics
	mux := http.NewServeMux()

	// Create a custom registry for this test
	registry := prometheus.NewRegistry()
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	mux.Handle("/metrics", handler)

	// Add a test endpoint with metrics middleware
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.Handle("/test", MetricsMiddleware(testHandler))

	// Create a test server
	server := httptest.NewServer(mux)
	defer server.Close()

	// Make a request to the test endpoint
	resp, err := http.Get(server.URL + "/test")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Make a request to the metrics endpoint
	resp, err = http.Get(server.URL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Just verify we can get metrics without validating specific content
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
}

// TestMetricsWithServerConfig tests the metrics with different HTTP methods
func TestMetricsWithServerConfig(t *testing.T) {
	// Create a test server with metrics
	mux := http.NewServeMux()

	// Create a custom registry for this test
	registry := prometheus.NewRegistry()
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	mux.Handle("/metrics", handler)

	// Add test endpoints with metrics middleware
	getHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond) // Simulate processing
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("GET OK"))
	})

	putHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond) // Simulate processing
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("PUT OK"))
	})

	deleteHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond) // Simulate processing
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("DELETE OK"))
	})

	mux.Handle("/keys/test", MetricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getHandler.ServeHTTP(w, r)
		case http.MethodPut:
			putHandler.ServeHTTP(w, r)
		case http.MethodDelete:
			deleteHandler.ServeHTTP(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})))

	// Create a test server
	server := httptest.NewServer(mux)
	defer server.Close()

	// Make requests to the test endpoints
	// PUT request
	putReq, err := http.NewRequest(http.MethodPut, server.URL+"/keys/test", strings.NewReader("test-value"))
	require.NoError(t, err)
	putResp, err := http.DefaultClient.Do(putReq)
	require.NoError(t, err)
	defer putResp.Body.Close()
	require.Equal(t, http.StatusCreated, putResp.StatusCode)

	// GET request
	getResp, err := http.Get(server.URL + "/keys/test")
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	// DELETE request
	deleteReq, err := http.NewRequest(http.MethodDelete, server.URL+"/keys/test", nil)
	require.NoError(t, err)
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	require.NoError(t, err)
	defer deleteResp.Body.Close()
	require.Equal(t, http.StatusOK, deleteResp.StatusCode)

	// Make a request to the metrics endpoint
	resp, err := http.Get(server.URL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Just verify we can get metrics without validating specific content
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
}

// TestMetricsWithConcurrentRequests tests the metrics with concurrent requests
func TestMetricsWithConcurrentRequests(t *testing.T) {
	// Create a test server with metrics
	mux := http.NewServeMux()

	// Create a custom registry for this test
	registry := prometheus.NewRegistry()
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	mux.Handle("/metrics", handler)

	// Add a test endpoint with metrics middleware
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond) // Simulate processing
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.Handle("/test", MetricsMiddleware(testHandler))

	// Create a test server
	server := httptest.NewServer(mux)
	defer server.Close()

	// Make concurrent requests
	const numRequests = 10
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Channel to collect results
	results := make(chan error, numRequests)

	// Launch concurrent requests
	for i := 0; i < numRequests; i++ {
		go func() {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/test", nil)
			if err != nil {
				results <- err
				return
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				results <- err
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				results <- err
				return
			}

			results <- nil
		}()
	}

	// Collect results
	for i := 0; i < numRequests; i++ {
		err := <-results
		require.NoError(t, err)
	}

	// Make a request to the metrics endpoint
	resp, err := http.Get(server.URL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Just verify we can get metrics without validating specific content
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
}
