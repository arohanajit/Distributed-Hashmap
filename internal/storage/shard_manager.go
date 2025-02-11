package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const (
	// DefaultVirtualNodes is the default number of virtual nodes per physical node
	DefaultVirtualNodes = 100
	// DefaultReplicationFactor is the default number of replicas for each key
	DefaultReplicationFactor = 1
)

// ShardManager handles the distribution of data across shards using consistent hashing
type ShardManager struct {
	mu                sync.RWMutex
	hashRing          []uint32            // Sorted list of hash values
	shardMapping      map[uint32]string   // Maps hash values to node addresses
	virtualNodes      map[string][]uint32 // Maps physical nodes to their virtual node hashes
	virtualNodeCount  int                 // Number of virtual nodes per physical node
	replicationFactor int                 // Number of replicas for each key
}

// ShardManagerOption is a function type for configuring ShardManager
type ShardManagerOption func(*ShardManager)

// WithVirtualNodes sets the number of virtual nodes per physical node
func WithVirtualNodes(count int) ShardManagerOption {
	return func(sm *ShardManager) {
		sm.virtualNodeCount = count
	}
}

// WithReplication sets the replication factor for keys
func WithReplication(factor int) ShardManagerOption {
	return func(sm *ShardManager) {
		sm.replicationFactor = factor
	}
}

// NewShardManager creates a new instance of ShardManager
func NewShardManager(opts ...ShardManagerOption) *ShardManager {
	sm := &ShardManager{
		hashRing:          make([]uint32, 0),
		shardMapping:      make(map[uint32]string),
		virtualNodes:      make(map[string][]uint32),
		virtualNodeCount:  DefaultVirtualNodes,
		replicationFactor: DefaultReplicationFactor,
	}

	for _, opt := range opts {
		opt(sm)
	}

	return sm
}

// hash generates a hash value for a given key
func (sm *ShardManager) hash(key string) uint32 {
	h := sha256.New()
	h.Write([]byte(key))
	hash := h.Sum(nil)
	// Use first 4 bytes of SHA-256 hash as uint32
	return binary.BigEndian.Uint32(hash[:4])
}

// generateVirtualNodeHash generates a hash for a virtual node
func (sm *ShardManager) generateVirtualNodeHash(nodeAddr string, vnode int) uint32 {
	key := fmt.Sprintf("%s-%d", nodeAddr, vnode)
	return sm.hash(key)
}

// AddNode adds a new node to the hash ring
func (sm *ShardManager) AddNode(nodeAddr string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Generate hashes for virtual nodes
	virtualHashes := make([]uint32, 0, sm.virtualNodeCount)
	for i := 0; i < sm.virtualNodeCount; i++ {
		hash := sm.generateVirtualNodeHash(nodeAddr, i)
		virtualHashes = append(virtualHashes, hash)
		sm.shardMapping[hash] = nodeAddr
	}

	// Store virtual node hashes for this physical node
	sm.virtualNodes[nodeAddr] = virtualHashes

	// Add virtual node hashes to ring and sort
	sm.hashRing = append(sm.hashRing, virtualHashes...)
	sort.Slice(sm.hashRing, func(i, j int) bool {
		return sm.hashRing[i] < sm.hashRing[j]
	})
}

// RemoveNode removes a node from the hash ring
func (sm *ShardManager) RemoveNode(nodeAddr string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Get virtual node hashes for this physical node
	virtualHashes, exists := sm.virtualNodes[nodeAddr]
	if !exists {
		return
	}

	// Remove virtual nodes from ring
	newRing := make([]uint32, 0, len(sm.hashRing)-len(virtualHashes))
	for _, hash := range sm.hashRing {
		isVirtualNode := false
		for _, vhash := range virtualHashes {
			if hash == vhash {
				isVirtualNode = true
				break
			}
		}
		if !isVirtualNode {
			newRing = append(newRing, hash)
		}
	}

	// Remove mappings
	for _, hash := range virtualHashes {
		delete(sm.shardMapping, hash)
	}
	delete(sm.virtualNodes, nodeAddr)

	sm.hashRing = newRing
}

// GetResponsibleNodes returns the nodes responsible for a given key
func (sm *ShardManager) GetResponsibleNodes(key string) []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if len(sm.hashRing) == 0 {
		return nil
	}

	// Find the first node
	hash := sm.hash(key)
	pos := sort.Search(len(sm.hashRing), func(i int) bool {
		return sm.hashRing[i] >= hash
	})
	if pos == len(sm.hashRing) {
		pos = 0
	}

	// Get the required number of unique nodes
	nodes := make([]string, 0, sm.replicationFactor)
	seen := make(map[string]bool)
	startPos := pos

	for len(nodes) < sm.replicationFactor && len(nodes) < len(sm.virtualNodes) {
		node := sm.shardMapping[sm.hashRing[pos]]
		if !seen[node] {
			seen[node] = true
			nodes = append(nodes, node)
		}
		pos = (pos + 1) % len(sm.hashRing)
		if pos == startPos {
			break
		}
	}

	return nodes
}

// GetResponsibleNode returns the primary node responsible for a given key
func (sm *ShardManager) GetResponsibleNode(key string) string {
	nodes := sm.GetResponsibleNodes(key)
	if len(nodes) == 0 {
		return ""
	}
	return nodes[0]
}

// GetAllNodes returns all physical nodes in the ring
func (sm *ShardManager) GetAllNodes() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	nodes := make([]string, 0, len(sm.virtualNodes))
	for node := range sm.virtualNodes {
		nodes = append(nodes, node)
	}
	return nodes
}
