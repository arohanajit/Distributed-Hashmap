package config

import (
	"os"
	"strconv"
	"time"
)

// ServerConfig holds all configuration settings for the server
type ServerConfig struct {
	// Server settings
	Port              int    `json:"port"`
	Host              string `json:"host"`
	MaxPayloadSize    int64  `json:"max_payload_size"`
	ShardCount        int    `json:"shard_count"`
	ReplicationFactor int    `json:"replication_factor"` // N: Number of copies per key

	// Storage settings
	StoragePath string `json:"storage_path"`

	// Shard/cluster settings
	ShardReplicas int    // Number of virtual nodes per physical node.
	ClusterNodes  string // Comma-separated list of initial node addresses.
	NodeID        string // Unique identifier for the current node (e.g., hostname or IP).

	// Replication settings
	WriteQuorum       int           `json:"write_quorum"`       // W: Minimum nodes required to acknowledge writes
	HeartbeatInterval time.Duration `json:"heartbeat_interval"` // Time between node health checks
}

// DefaultConfig returns a ServerConfig with default values
func DefaultConfig() *ServerConfig {
	return &ServerConfig{
		Port:              8080,
		Host:              "0.0.0.0",
		MaxPayloadSize:    1024 * 1024, // 1MB
		ShardCount:        4,
		ReplicationFactor: 3, // Default to 3 replicas (N)
		StoragePath:       "./data",
		ShardReplicas:     10,
		ClusterNodes:      "",
		NodeID:            "",
		WriteQuorum:       2,               // Default to majority (W = N/2 + 1)
		HeartbeatInterval: 5 * time.Second, // Default to 5 seconds
	}
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *ServerConfig {
	config := DefaultConfig()

	if port := os.Getenv("SERVER_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			config.Port = p
		}
	}

	if host := os.Getenv("SERVER_HOST"); host != "" {
		config.Host = host
	}

	if maxSize := os.Getenv("MAX_PAYLOAD_SIZE"); maxSize != "" {
		if size, err := strconv.ParseInt(maxSize, 10, 64); err == nil {
			config.MaxPayloadSize = size
		}
	}

	if shardCount := os.Getenv("SHARD_COUNT"); shardCount != "" {
		if count, err := strconv.Atoi(shardCount); err == nil {
			config.ShardCount = count
		}
	}

	if replicationFactor := os.Getenv("REPLICATION_FACTOR"); replicationFactor != "" {
		if factor, err := strconv.Atoi(replicationFactor); err == nil {
			config.ReplicationFactor = factor
		}
	}

	if storagePath := os.Getenv("STORAGE_PATH"); storagePath != "" {
		config.StoragePath = storagePath
	}

	if shardReplicas := os.Getenv("SHARD_REPLICAS"); shardReplicas != "" {
		if replicas, err := strconv.Atoi(shardReplicas); err == nil {
			config.ShardReplicas = replicas
		}
	}

	if clusterNodes := os.Getenv("CLUSTER_NODES"); clusterNodes != "" {
		config.ClusterNodes = clusterNodes
	}

	if nodeID := os.Getenv("NODE_ID"); nodeID != "" {
		config.NodeID = nodeID
	}

	// Load new replication settings
	if writeQuorum := os.Getenv("WRITE_QUORUM"); writeQuorum != "" {
		if quorum, err := strconv.Atoi(writeQuorum); err == nil {
			config.WriteQuorum = quorum
		}
	}

	if heartbeatInterval := os.Getenv("HEARTBEAT_INTERVAL"); heartbeatInterval != "" {
		if duration, err := time.ParseDuration(heartbeatInterval); err == nil {
			config.HeartbeatInterval = duration
		}
	}

	return config
}

// Validate checks if the configuration is valid
func (c *ServerConfig) Validate() error {
	// Ensure write quorum is not larger than replication factor
	if c.WriteQuorum > c.ReplicationFactor {
		c.WriteQuorum = c.ReplicationFactor
	}

	// Ensure write quorum is at least majority
	minQuorum := (c.ReplicationFactor / 2) + 1
	if c.WriteQuorum < minQuorum {
		c.WriteQuorum = minQuorum
	}

	return nil
}
