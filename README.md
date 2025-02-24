# Distributed Hashmap

A distributed key-value store implementation in Go, featuring consistent hashing, data replication, fault tolerance, and dynamic service discovery.

## Features

- Distributed key-value storage
- Consistent hashing for data distribution
- Data replication and fault tolerance
- REST and gRPC APIs
- Client libraries in multiple languages
- Dynamic cluster membership management
- Service discovery (etcd/Consul integration and SWIM gossip protocol)
- Failure detection with configurable health checks

## Architecture

The system is built with the following components:

- **Storage Layer**: Handles data storage and sharding
- **Cluster Management**: Manages node discovery and health checking
- **Service Discovery**: Supports both centralized (etcd/Consul) and decentralized (SWIM gossip) discovery
- **Replication**: Ensures data redundancy and consistency
- **Failure Detection**: Detects node failures and initiates recovery
- **API Layer**: Provides REST and gRPC interfaces

## Getting Started

### Prerequisites

- Go 1.21 or later
- Make
- Docker (optional)
- etcd (optional, for centralized service discovery)

### Building

```bash
# Build all components
make build

# Run tests
make test

# Clean build artifacts
make clean
```

### Running

```bash
# Start the server with a node-id
./build/dhashmap-server -node-id "node1"

# Start with etcd discovery
./build/dhashmap-server -node-id "node1" -discovery "etcd" -etcd-endpoints "localhost:2379"

# Start with SWIM gossip discovery
./build/dhashmap-server -node-id "node1" -discovery "swim"

# Join an existing cluster with SWIM
./build/dhashmap-server -node-id "node2" -discovery "swim" -seed-nodes "node1:7946"

# Run the example client (if implemented)
make run-client
```

### Docker

```bash
# Build the Docker image
docker build -t dhashmap .

# Run the container
docker run -p 8080:8080 dhashmap

# Run with Docker Compose (for multi-node setup)
docker-compose up
```

## Service Discovery

The system supports two modes of service discovery:

### etcd/Consul Integration

For centralized service discovery, the system can integrate with etcd or Consul. This allows nodes to register themselves and discover other nodes through a central registry.

```bash
# Start with etcd discovery
export ETCD_ENDPOINTS="localhost:2379"
./build/dhashmap-server -node-id "node1" -discovery "etcd"
```

### SWIM Gossip Protocol

For decentralized service discovery, the system implements the SWIM (Scalable Weakly-consistent Infection-style Process Group Membership) gossip protocol. This allows nodes to discover each other without a central registry.

```bash
# Start the first node
./build/dhashmap-server -node-id "node1" -discovery "swim"

# Join an existing cluster
./build/dhashmap-server -node-id "node2" -discovery "swim" -seed-nodes "node1:7946"
```

## Failure Detection

The system includes a robust failure detection mechanism that:

- Monitors node health through regular health checks
- Detects failed nodes and removes them from the cluster
- Triggers data re-replication when necessary
- Supports both direct and indirect (gossip-based) health checks

## Configuration

Configuration can be done through command-line flags, environment variables, or configuration files:

```bash
# Command-line flags
./build/dhashmap-server -node-id "node1" -port 8080 -discovery "swim"

# Environment variables
export NODE_ID="node1"
export PORT="8080" 
export DISCOVERY_MODE="swim"
./build/dhashmap-server

# Configuration file
./build/dhashmap-server -config config.yaml
```

## API Documentation

API documentation can be found in the `docs` directory.

## Development

### Project Structure

```
.
├── cmd/                # Command-line applications
│   ├── client/         # Client CLI
│   └── server/         # Server application
├── internal/           # Internal packages
│   ├── api/            # API handlers
│   ├── cluster/        # Cluster management
│   ├── config/         # Configuration
│   ├── storage/        # Storage implementation
│   └── utils/          # Utility functions
├── pkg/                # Public packages for external use
├── scripts/            # Build and utility scripts
└── docs/               # Documentation
```

### Running Tests

```bash
# Run all tests
make test

# Run specific component tests
./scripts/test_components.sh

# Run with etcd integration tests (requires running etcd)
TEST_WITH_ETCD=true go test ./internal/cluster/...

# Run with SWIM tests
TEST_WITH_SWIM=true go test ./internal/cluster/...
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.