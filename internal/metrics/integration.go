package metrics

import (
	"sync"
	"time"
)

// ClusterMetricsCollector provides methods to collect metrics related to cluster operations
type ClusterMetricsCollector struct {
	metrics        *PrometheusMetrics
	nodesMutex     sync.RWMutex
	rebalanceMutex sync.RWMutex
	nodeCount      int
	pendingKeys    int
}

// NewClusterMetricsCollector creates a new ClusterMetricsCollector
func NewClusterMetricsCollector() *ClusterMetricsCollector {
	collector := &ClusterMetricsCollector{
		metrics:     GetMetrics(),
		nodeCount:   0,
		pendingKeys: 0,
	}
	// Initialize metrics
	collector.metrics.SetClusterNodesTotal(0)
	collector.metrics.SetRebalanceKeysPending(0)
	return collector
}

// GetNodeCount returns the current node count
func (c *ClusterMetricsCollector) GetNodeCount() int {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()

	return c.nodeCount
}

// UpdateNodeCount updates the total number of nodes in the cluster
func (c *ClusterMetricsCollector) UpdateNodeCount(count int) {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	c.nodeCount = count
	c.metrics.SetClusterNodesTotal(count)
}

// AddNode increments the node count
func (c *ClusterMetricsCollector) AddNode() {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	c.nodeCount++
	c.metrics.SetClusterNodesTotal(c.nodeCount)
}

// RemoveNode decrements the node count
func (c *ClusterMetricsCollector) RemoveNode() {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	if c.nodeCount > 0 {
		c.nodeCount--
	}
	c.metrics.SetClusterNodesTotal(c.nodeCount)
}

// StartRebalance initializes the rebalance metrics with the total number of keys to be rebalanced
func (c *ClusterMetricsCollector) StartRebalance(totalKeys int) {
	c.rebalanceMutex.Lock()
	defer c.rebalanceMutex.Unlock()

	c.pendingKeys = totalKeys
	c.metrics.SetRebalanceKeysPending(totalKeys)
}

// UpdateRebalanceProgress updates the number of keys pending rebalance
func (c *ClusterMetricsCollector) UpdateRebalanceProgress(pendingKeys int) {
	c.rebalanceMutex.Lock()
	defer c.rebalanceMutex.Unlock()

	c.pendingKeys = pendingKeys
	c.metrics.SetRebalanceKeysPending(pendingKeys)
}

// CompleteRebalance marks the rebalance as complete by setting pending keys to 0
func (c *ClusterMetricsCollector) CompleteRebalance() {
	c.rebalanceMutex.Lock()
	defer c.rebalanceMutex.Unlock()

	c.pendingKeys = 0
	c.metrics.SetRebalanceKeysPending(0)
}

// RecordReplicationError records a replication error for a specific node
func (c *ClusterMetricsCollector) RecordReplicationError(node string) {
	c.metrics.RecordReplicationError(node)
}

// UpdateReplicaLag updates the lag time for a specific node's replicas
func (c *ClusterMetricsCollector) UpdateReplicaLag(node string, lag time.Duration) {
	c.metrics.SetReplicaLag(node, lag.Seconds())
}

// Example usage in a rebalancing operation:
//
// func (s *ShardManager) RebalanceShards() error {
//     // Get metrics collector
//     collector := metrics.NewClusterMetricsCollector()
//
//     // Calculate keys to transfer
//     keysToTransfer := s.calculateKeysToTransfer()
//
//     // Start rebalance metrics
//     collector.StartRebalance(len(keysToTransfer))
//
//     // Process keys in batches
//     batchSize := s.config.RebalanceBatchSize
//     for i := 0; i < len(keysToTransfer); i += batchSize {
//         end := i + batchSize
//         if end > len(keysToTransfer) {
//             end = len(keysToTransfer)
//         }
//
//         batch := keysToTransfer[i:end]
//         err := s.transferKeyBatch(batch)
//         if err != nil {
//             return err
//         }
//
//         // Update progress
//         collector.UpdateRebalanceProgress(len(batch))
//     }
//
//     // Complete rebalance
//     collector.CompleteRebalance()
//     return nil
// }
