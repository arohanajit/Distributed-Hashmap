# Metrics Package

This package provides Prometheus metrics integration for the Distributed Hashmap system. It allows monitoring of various aspects of the system, including cluster health, request handling, storage usage, and replication status.

## Available Metrics

### Cluster Metrics
- `cluster_nodes_total`: The total number of nodes in the cluster
- `rebalance_keys_pending`: The number of keys awaiting transfer during rebalancing

### HTTP Metrics
- `requests_total`: The total number of processed requests (labeled by method, endpoint, status)
- `request_duration_seconds`: Request latencies in seconds (labeled by method, endpoint)
- `requests_in_flight`: The number of requests currently being processed

### Storage Metrics
- `keys_total`: The total number of keys stored
- `storage_bytes_used`: The total number of bytes used for storage

### Replication Metrics
- `replication_errors_total`: The total number of replication errors (labeled by node)
- `replica_lag_seconds`: Lag time in seconds for replicas to be updated (labeled by node)

## Components

### PrometheusMetrics

The `PrometheusMetrics` struct is the core component that defines and manages all metrics. It's implemented as a singleton to ensure consistent metrics collection across the application.

```go
metrics := metrics.GetMetrics()
```

### ClusterMetricsCollector

The `ClusterMetricsCollector` provides a higher-level API for updating cluster-related metrics:

```go
collector := metrics.NewClusterMetricsCollector()

// Update node count
collector.UpdateNodeCount(5)

// Track rebalance operations
collector.StartRebalance(100)
collector.UpdateRebalanceProgress(50)
collector.CompleteRebalance()

// Record replication metrics
collector.RecordReplicationError("node1")
collector.UpdateReplicaLag("node1", 500*time.Millisecond)
```

### HTTP Middleware

The package includes middleware for automatically tracking HTTP request metrics:

```go
// Wrap a handler function
http.Handle("/api/endpoint", metrics.MetricsMiddleware(handlerFunc))

// Or use the WrapHandlerFunc helper
http.HandleFunc("/api/endpoint", metrics.WrapHandlerFunc(handlerFunc))
```

## Metrics Endpoint

The metrics are exposed via a standard Prometheus endpoint at `/metrics`. To register this endpoint:

```go
// With standard http.ServeMux
mux := http.NewServeMux()
metrics.RegisterMetricsHandler(mux)

// With gorilla/mux
router := mux.NewRouter()
metricsMux := http.NewServeMux()
metrics.RegisterMetricsHandler(metricsMux)
router.PathPrefix("/metrics").Handler(metricsMux)
```

## Integration with Prometheus

To scrape these metrics with Prometheus, add a scrape configuration to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'distributed-hashmap'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:8080']
```

## Example Dashboard

A Grafana dashboard for these metrics might include:

- Cluster health: Node count, rebalance status
- Request performance: Request rate, latency histograms, error rates
- Storage: Key count, storage usage
- Replication: Replication errors, replica lag

## Example Usage

See `example.go` for a complete example of how to set up a server with metrics integration. 