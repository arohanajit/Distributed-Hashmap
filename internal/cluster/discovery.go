package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// DiscoveryManager handles node discovery and management
// It uses the common Node and NodeStatus types defined in types.go

type DiscoveryManager struct {
	self  Node
	nodes map[string]Node
}

// NewDiscoveryManager creates a new DiscoveryManager instance
func NewDiscoveryManager(self Node) *DiscoveryManager {
	return &DiscoveryManager{
		self:  self,
		nodes: make(map[string]Node),
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

// AddNode adds a new node to the discovery manager
func (dm *DiscoveryManager) AddNode(node Node) error {
	if node.ID == "" || node.Address == "" {
		return fmt.Errorf("invalid node: ID and address are required")
	}

	dm.nodes[node.ID] = node
	return nil
}

// RemoveNode removes a node from the discovery manager
func (dm *DiscoveryManager) RemoveNode(nodeID string) error {
	if _, exists := dm.nodes[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	delete(dm.nodes, nodeID)
	return nil
}

// GetNodes returns all known nodes
func (dm *DiscoveryManager) GetNodes() []Node {
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
