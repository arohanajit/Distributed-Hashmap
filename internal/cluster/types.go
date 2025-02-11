package cluster

// NodeStatus represents the current status of a node in the cluster
type NodeStatus int

const (
	// NodeStatusActive indicates the node is active and participating in the cluster
	NodeStatusActive NodeStatus = iota
	// NodeStatusRemoved indicates the node has been removed from the cluster
	NodeStatusRemoved
)

// Node represents a node in the distributed system
type Node struct {
	// ID is the unique identifier of the node
	ID string
	// Address is the network address of the node (host:port)
	Address string
	// Status represents the current status of the node
	Status NodeStatus
}
