package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// DiscoveryManager handles node discovery and management
// It uses the common Node and NodeStatus types defined in types.go

type DiscoveryManager struct {
	mu             sync.RWMutex
	self           Node
	nodes          map[string]Node
	etcdClient     *clientv3.Client
	watchCancel    context.CancelFunc
	clusterManager *ClusterManagerImpl
	servicePrefix  string
	leaseTTL       int64
	leaseID        clientv3.LeaseID
}

// DiscoveryConfig contains configuration for the DiscoveryManager
type DiscoveryConfig struct {
	// EtcdEndpoints is a list of etcd endpoints
	EtcdEndpoints []string
	// ServicePrefix is the prefix used for service registration in etcd
	ServicePrefix string
	// LeaseTTL is the time-to-live (in seconds) for the etcd lease
	LeaseTTL int64
}

// NewDiscoveryManager creates a new DiscoveryManager instance
func NewDiscoveryManager(self Node) *DiscoveryManager {
	return &DiscoveryManager{
		self:          self,
		nodes:         make(map[string]Node),
		servicePrefix: "/services/hashmap/nodes/",
		leaseTTL:      30, // 30 seconds default
	}
}

// LoadNodesFromEnv loads initial nodes from environment variables
func (dm *DiscoveryManager) LoadNodesFromEnv() error {
	nodesStr := os.Getenv("CLUSTER_NODES")
	if nodesStr == "" {
		return nil
	}

	nodeAddrs := strings.Split(nodesStr, ",")
	for i, addr := range nodeAddrs {
		node := Node{
			ID:      fmt.Sprintf("node-%d", i),
			Address: strings.TrimSpace(addr),
			Status:  NodeStatusActive,
		}
		dm.nodes[node.ID] = node
	}

	return nil
}

// ConnectToEtcd connects to etcd using the provided configuration
func (dm *DiscoveryManager) ConnectToEtcd(config DiscoveryConfig) error {
	if len(config.EtcdEndpoints) == 0 {
		return fmt.Errorf("no etcd endpoints provided")
	}

	cfg := clientv3.Config{
		Endpoints:   config.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	}

	client, err := clientv3.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to etcd: %v", err)
	}

	dm.mu.Lock()
	dm.etcdClient = client
	if config.ServicePrefix != "" {
		dm.servicePrefix = config.ServicePrefix
	}
	if config.LeaseTTL > 0 {
		dm.leaseTTL = config.LeaseTTL
	}
	dm.mu.Unlock()

	// Load initial nodes from etcd
	if err := dm.loadNodesFromEtcd(); err != nil {
		return fmt.Errorf("failed to load nodes from etcd: %v", err)
	}

	// Start watching for changes
	dm.startWatchingEtcd()

	return nil
}

// RegisterWithEtcd registers the current node with etcd
func (dm *DiscoveryManager) RegisterWithEtcd(ctx context.Context) error {
	if dm.etcdClient == nil {
		return fmt.Errorf("etcd client not initialized")
	}

	// Create a lease
	lease, err := dm.etcdClient.Grant(ctx, dm.leaseTTL)
	if err != nil {
		return fmt.Errorf("failed to create lease: %v", err)
	}

	dm.mu.Lock()
	dm.leaseID = lease.ID
	dm.mu.Unlock()

	// Store node information under the lease
	nodeKey := dm.servicePrefix + dm.self.ID
	nodeData, err := json.Marshal(dm.self)
	if err != nil {
		return fmt.Errorf("failed to marshal node data: %v", err)
	}

	_, err = dm.etcdClient.Put(ctx, nodeKey, string(nodeData), clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to register node: %v", err)
	}

	// Keep the lease alive
	keepAliveCh, err := dm.etcdClient.KeepAlive(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("failed to keep lease alive: %v", err)
	}

	// Monitor the keep-alive channel in a goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ka, ok := <-keepAliveCh:
				if !ok {
					// Reconnect if keep-alive failed
					dm.registerWithRetry(ctx)
					return
				}
				if ka == nil {
					dm.registerWithRetry(ctx)
					return
				}
			}
		}
	}()

	return nil
}

// registerWithRetry attempts to re-register with etcd with exponential backoff
func (dm *DiscoveryManager) registerWithRetry(ctx context.Context) {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			if err := dm.RegisterWithEtcd(ctx); err == nil {
				return
			}

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// loadNodesFromEtcd loads all nodes from etcd
func (dm *DiscoveryManager) loadNodesFromEtcd() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := dm.etcdClient.Get(ctx, dm.servicePrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get nodes from etcd: %v", err)
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Clear existing nodes loaded from etcd (not from env vars)
	for id := range dm.nodes {
		if !strings.HasPrefix(id, "node-") {
			delete(dm.nodes, id)
		}
	}

	for _, kv := range resp.Kvs {
		var node Node
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			continue
		}

		// Skip self
		if node.ID == dm.self.ID {
			continue
		}

		dm.nodes[node.ID] = node

		// If cluster manager is set, add the node
		if dm.clusterManager != nil {
			go func(n Node) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				dm.clusterManager.JoinNode(ctx, n)
			}(node)
		}
	}

	return nil
}

// startWatchingEtcd watches for changes in the etcd registry
func (dm *DiscoveryManager) startWatchingEtcd() {
	// Cancel any previous watch
	if dm.watchCancel != nil {
		dm.watchCancel()
	}

	ctx, cancel := context.WithCancel(context.Background())
	dm.watchCancel = cancel

	watchChan := dm.etcdClient.Watch(ctx, dm.servicePrefix, clientv3.WithPrefix())

	go func() {
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				nodeKey := string(event.Kv.Key)
				nodeID := strings.TrimPrefix(nodeKey, dm.servicePrefix)

				switch event.Type {
				case clientv3.EventTypePut:
					var node Node
					if err := json.Unmarshal(event.Kv.Value, &node); err != nil {
						continue
					}

					// Skip self
					if node.ID == dm.self.ID {
						continue
					}

					dm.mu.Lock()
					dm.nodes[node.ID] = node
					dm.mu.Unlock()

					// Notify cluster manager of new node
					if dm.clusterManager != nil {
						go func(n Node) {
							ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
							defer cancel()
							dm.clusterManager.JoinNode(ctx, n)
						}(node)
					}

				case clientv3.EventTypeDelete:
					dm.mu.Lock()
					delete(dm.nodes, nodeID)
					dm.mu.Unlock()

					// Notify cluster manager of node removal
					if dm.clusterManager != nil {
						go func(id string) {
							ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
							defer cancel()
							dm.clusterManager.LeaveNode(ctx, id)
						}(nodeID)
					}
				}
			}
		}
	}()
}

// StopDiscovery stops the discovery service and unregisters from etcd
func (dm *DiscoveryManager) StopDiscovery(ctx context.Context) error {
	// Cancel the watch
	if dm.watchCancel != nil {
		dm.watchCancel()
		dm.watchCancel = nil
	}

	// Revoke the lease to unregister
	if dm.etcdClient != nil && dm.leaseID != 0 {
		_, err := dm.etcdClient.Revoke(ctx, dm.leaseID)
		if err != nil {
			return fmt.Errorf("failed to revoke lease: %v", err)
		}

		// Close etcd client
		if err := dm.etcdClient.Close(); err != nil {
			return fmt.Errorf("failed to close etcd client: %v", err)
		}
	}

	return nil
}

// SetClusterManager sets the cluster manager to be notified of node changes
func (dm *DiscoveryManager) SetClusterManager(cm *ClusterManagerImpl) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.clusterManager = cm
}

// AddNode adds a new node to the discovery manager
func (dm *DiscoveryManager) AddNode(node Node) error {
	if node.ID == "" || node.Address == "" {
		return fmt.Errorf("invalid node: ID and address are required")
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.nodes[node.ID] = node
	return nil
}

// RemoveNode removes a node from the discovery manager
func (dm *DiscoveryManager) RemoveNode(nodeID string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if _, exists := dm.nodes[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	delete(dm.nodes, nodeID)
	return nil
}

// GetNodes returns all known nodes
func (dm *DiscoveryManager) GetNodes() []Node {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	nodes := make([]Node, 0, len(dm.nodes))
	for _, node := range dm.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetNodesJSON returns all known nodes as JSON
func (dm *DiscoveryManager) GetNodesJSON() ([]byte, error) {
	return json.Marshal(dm.GetNodes())
}

// GetSelf returns the current node's information
func (dm *DiscoveryManager) GetSelf() Node {
	return dm.self
}
