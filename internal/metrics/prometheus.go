package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Singleton instance
	instance *PrometheusMetrics
	once     sync.Once
)

// PrometheusMetrics handles all metrics collection for the distributed hashmap
type PrometheusMetrics struct {
	// Cluster metrics
	ClusterNodesTotal prometheus.Gauge

	// Rebalance metrics
	RebalanceKeysPending prometheus.Gauge

	// Operation metrics
	RequestsTotal    *prometheus.CounterVec
	RequestDuration  *prometheus.HistogramVec
	RequestsInFlight prometheus.Gauge

	// Storage metrics
	KeysTotal        prometheus.Gauge
	StorageBytesUsed prometheus.Gauge

	// Replication metrics
	ReplicationErrors *prometheus.CounterVec
	ReplicaLag        *prometheus.GaugeVec
}

// NewPrometheusMetrics creates a new PrometheusMetrics instance
func NewPrometheusMetrics() *PrometheusMetrics {
	once.Do(func() {
		instance = &PrometheusMetrics{
			// Cluster metrics
			ClusterNodesTotal: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "cluster_nodes_total",
				Help: "The total number of nodes in the cluster",
			}),

			// Rebalance metrics
			RebalanceKeysPending: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "rebalance_keys_pending",
				Help: "The number of keys awaiting transfer during rebalancing",
			}),

			// Operation metrics
			RequestsTotal: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "requests_total",
					Help: "The total number of processed requests",
				},
				[]string{"method", "endpoint", "status"},
			),
			RequestDuration: promauto.NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "request_duration_seconds",
					Help:    "The request latencies in seconds",
					Buckets: prometheus.DefBuckets,
				},
				[]string{"method", "endpoint"},
			),
			RequestsInFlight: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "requests_in_flight",
				Help: "The number of requests currently being processed",
			}),

			// Storage metrics
			KeysTotal: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "keys_total",
				Help: "The total number of keys stored",
			}),
			StorageBytesUsed: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "storage_bytes_used",
				Help: "The total number of bytes used for storage",
			}),

			// Replication metrics
			ReplicationErrors: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "replication_errors_total",
					Help: "The total number of replication errors",
				},
				[]string{"node"},
			),
			ReplicaLag: promauto.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "replica_lag_seconds",
					Help: "The lag time in seconds for replicas to be updated",
				},
				[]string{"node"},
			),
		}
	})

	return instance
}

// GetMetrics returns the singleton PrometheusMetrics instance
func GetMetrics() *PrometheusMetrics {
	if instance == nil {
		return NewPrometheusMetrics()
	}
	return instance
}

// RegisterMetricsHandler registers the Prometheus metrics handler with the HTTP server
func RegisterMetricsHandler(mux *http.ServeMux) {
	mux.Handle("/metrics", promhttp.Handler())
}

// SetClusterNodesTotal updates the total number of nodes in the cluster
func (pm *PrometheusMetrics) SetClusterNodesTotal(count int) {
	pm.ClusterNodesTotal.Set(float64(count))
}

// SetRebalanceKeysPending updates the number of keys pending rebalance
func (pm *PrometheusMetrics) SetRebalanceKeysPending(count int) {
	pm.RebalanceKeysPending.Set(float64(count))
}

// IncRebalanceKeysPending increments the number of keys pending rebalance
func (pm *PrometheusMetrics) IncRebalanceKeysPending(delta int) {
	pm.RebalanceKeysPending.Add(float64(delta))
}

// RecordRequest records a request with its method, endpoint, and status
func (pm *PrometheusMetrics) RecordRequest(method, endpoint, status string) {
	pm.RequestsTotal.WithLabelValues(method, endpoint, status).Inc()
}

// ObserveRequestDuration records the duration of a request
func (pm *PrometheusMetrics) ObserveRequestDuration(method, endpoint string, duration float64) {
	pm.RequestDuration.WithLabelValues(method, endpoint).Observe(duration)
}

// IncRequestsInFlight increments the number of requests in flight
func (pm *PrometheusMetrics) IncRequestsInFlight() {
	pm.RequestsInFlight.Inc()
}

// DecRequestsInFlight decrements the number of requests in flight
func (pm *PrometheusMetrics) DecRequestsInFlight() {
	pm.RequestsInFlight.Dec()
}

// SetKeysTotal updates the total number of keys stored
func (pm *PrometheusMetrics) SetKeysTotal(count int) {
	pm.KeysTotal.Set(float64(count))
}

// SetStorageBytesUsed updates the total bytes used for storage
func (pm *PrometheusMetrics) SetStorageBytesUsed(bytes int64) {
	pm.StorageBytesUsed.Set(float64(bytes))
}

// RecordReplicationError records a replication error for a specific node
func (pm *PrometheusMetrics) RecordReplicationError(node string) {
	pm.ReplicationErrors.WithLabelValues(node).Inc()
}

// SetReplicaLag sets the lag time for a specific node's replicas
func (pm *PrometheusMetrics) SetReplicaLag(node string, seconds float64) {
	pm.ReplicaLag.WithLabelValues(node).Set(seconds)
}
