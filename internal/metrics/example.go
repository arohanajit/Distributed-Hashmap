package metrics

import (
	"log"
	"net/http"
	"time"
)

// ExampleServerWithMetrics demonstrates how to set up a server with metrics
func ExampleServerWithMetrics() {
	// Create a new HTTP server mux
	mux := http.NewServeMux()

	// Register metrics handler
	RegisterMetricsHandler(mux)

	// Register application endpoints with metrics middleware
	mux.Handle("/api/get", WrapHandlerFunc(http.HandlerFunc(exampleGetHandler)))
	mux.Handle("/api/set", WrapHandlerFunc(http.HandlerFunc(exampleSetHandler)))

	// Create metrics collector for cluster operations
	collector := NewClusterMetricsCollector()

	// Update node count (this would typically be done when cluster membership changes)
	collector.UpdateNodeCount(3)

	// Simulate a rebalance operation in a separate goroutine
	go simulateRebalance(collector)

	// Start the server
	log.Println("Starting server with metrics on :8080")
	log.Println("Metrics available at http://localhost:8080/metrics")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

// Example handlers
func exampleGetHandler(w http.ResponseWriter, r *http.Request) {
	// Simulate processing
	time.Sleep(10 * time.Millisecond)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Value retrieved"))
}

func exampleSetHandler(w http.ResponseWriter, r *http.Request) {
	// Simulate processing
	time.Sleep(20 * time.Millisecond)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Value set"))
}

// simulateRebalance demonstrates how to use the metrics collector during a rebalance operation
func simulateRebalance(collector *ClusterMetricsCollector) {
	// Simulate a rebalance operation
	totalKeys := 100
	collector.StartRebalance(totalKeys)

	// Process keys in batches
	batchSize := 10
	for i := 0; i < totalKeys; i += batchSize {
		// Simulate batch processing
		time.Sleep(500 * time.Millisecond)

		// Update progress
		remainingKeys := totalKeys - (i + batchSize)
		if remainingKeys < 0 {
			remainingKeys = 0
		}
		collector.UpdateRebalanceProgress(remainingKeys)
	}

	// Complete rebalance
	collector.CompleteRebalance()
	log.Println("Rebalance operation completed")

	// Simulate replica lag and errors
	go func() {
		for {
			// Simulate replica lag
			collector.UpdateReplicaLag("node1", 100*time.Millisecond)
			collector.UpdateReplicaLag("node2", 200*time.Millisecond)

			// Occasionally simulate a replication error
			if time.Now().Second()%10 == 0 {
				collector.RecordReplicationError("node2")
			}

			time.Sleep(1 * time.Second)
		}
	}()
}
