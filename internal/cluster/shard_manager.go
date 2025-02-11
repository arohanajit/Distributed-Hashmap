package cluster

// ShardManager defines the interface for managing shards
type ShardManager interface {
	// GetResponsibleNodes returns the list of nodes responsible for a key
	GetResponsibleNodes(key string) []string

	// HasPrimaryShards checks if the current node has any primary shards
	HasPrimaryShards() bool

	// HasPrimaryShardsForNode checks if a specific node has primary shards
	HasPrimaryShardsForNode(nodeID string) bool

	// UpdateResponsibleNodes updates the list of nodes responsible for a key
	UpdateResponsibleNodes(key string, nodes []string)

	// GetNodeAddress returns the address of a node
	GetNodeAddress(nodeID string) string

	// GetAllNodes returns a list of all nodes in the cluster
	GetAllNodes() []string

	// GetCurrentNodeID returns the ID of the current node
	GetCurrentNodeID() string

	// GetNodeForKey returns the node responsible for a given key
	GetNodeForKey(key string) string

	// GetSuccessors returns the list of successor nodes for a given node
	GetSuccessors(nodeID string) []string

	// GetPredecessors returns the list of predecessor nodes for a given node
	GetPredecessors(nodeID string) []string

	// GetLocalNodeID returns the ID of the local node
	GetLocalNodeID() string

	// GetSuccessorNodes returns N successor nodes after the given node
	GetSuccessorNodes(nodeID string, count int) []string
}
