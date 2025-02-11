package cluster

import (
	"fmt"
)

// mockShardManager implements ShardManager interface for testing
type mockShardManager struct {
	responsibleNodes map[string][]string
	nodeAddresses    map[string]string
	currentNodeID    string
}

func newMockShardManager(currentNodeID string) *mockShardManager {
	return &mockShardManager{
		responsibleNodes: make(map[string][]string),
		nodeAddresses:    make(map[string]string),
		currentNodeID:    currentNodeID,
	}
}

func (m *mockShardManager) GetResponsibleNodes(key string) []string {
	nodes := m.responsibleNodes[key]
	fmt.Printf("Getting responsible nodes for key %s: %v\n", key, nodes)
	return nodes
}

func (m *mockShardManager) GetSuccessorNodes(nodeID string, count int) []string {
	allNodes := make([]string, 0, len(m.nodeAddresses))
	for n := range m.nodeAddresses {
		if n != nodeID {
			allNodes = append(allNodes, n)
		}
	}
	if count > len(allNodes) {
		count = len(allNodes)
	}
	return allNodes[:count]
}

func (m *mockShardManager) GetNodeAddress(nodeID string) string {
	return m.nodeAddresses[nodeID]
}

func (m *mockShardManager) GetAllNodes() []string {
	nodes := make([]string, 0, len(m.nodeAddresses))
	for nodeID := range m.nodeAddresses {
		nodes = append(nodes, nodeID)
	}
	fmt.Printf("Getting all nodes: %v\n", nodes)
	return nodes
}

func (m *mockShardManager) GetCurrentNodeID() string {
	return m.currentNodeID
}

func (m *mockShardManager) GetNodeForKey(key string) string {
	if nodes := m.GetResponsibleNodes(key); len(nodes) > 0 {
		return nodes[0]
	}
	return ""
}

func (m *mockShardManager) GetSuccessors(nodeID string) []string {
	return m.GetSuccessorNodes(nodeID, 2) // Default to 2 successors
}

func (m *mockShardManager) GetPredecessors(nodeID string) []string {
	// Simple implementation for testing
	return []string{nodeID + "-pred1", nodeID + "-pred2"}
}

func (m *mockShardManager) GetLocalNodeID() string {
	return m.currentNodeID
}

func (m *mockShardManager) HasPrimaryShards() bool {
	fmt.Printf("Checking if node %s has primary shards\n", m.currentNodeID)
	// Check if this node is the primary (first) node for any key
	for key, nodes := range m.responsibleNodes {
		fmt.Printf("Key %s has responsible nodes: %v\n", key, nodes)
		if len(nodes) > 0 && nodes[0] == m.currentNodeID {
			fmt.Printf("Node %s is primary for key %s\n", m.currentNodeID, key)
			return true
		}
	}
	fmt.Printf("Node %s has no primary shards\n", m.currentNodeID)
	return false
}

// HasPrimaryShardsForNode checks if the given node has any primary shards
func (m *mockShardManager) HasPrimaryShardsForNode(nodeID string) bool {
	fmt.Printf("Checking if node %s has primary shards\n", nodeID)
	for key, nodes := range m.responsibleNodes {
		if len(nodes) > 0 && nodes[0] == nodeID {
			fmt.Printf("Node %s is primary for key %s\n", nodeID, key)
			return true
		}
	}
	fmt.Printf("Node %s has no primary shards\n", nodeID)
	return false
}

func (m *mockShardManager) UpdateResponsibleNodes(key string, nodes []string) {
	fmt.Printf("Updating responsible nodes for key %s: %v\n", key, nodes)
	m.responsibleNodes[key] = nodes
}
