# Distributed Hashmap

A distributed key-value store implementation in Go, featuring consistent hashing, data replication, and fault tolerance.

## Features

- Distributed key-value storage
- Consistent hashing for data distribution
- Data replication and fault tolerance
- REST and gRPC APIs
- Client libraries in multiple languages
- Cluster management and node discovery

## Architecture

The system is built with the following components:

- Storage Layer: Handles data storage and sharding
- Cluster Management: Manages node discovery and health checking
- Replication: Ensures data redundancy and consistency
- API Layer: Provides REST and gRPC interfaces

## Getting Started

### Prerequisites

- Go 1.21 or later
- Make
- Docker (optional)

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

# Alternatively, modify the Makefile target run-server to include the -node-id flag, e.g.:
# run-server:
#	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-server ./cmd/server
#	./$(BUILD_DIR)/$(BINARY_NAME)-server -node-id "node1"

# Run the example client (if implemented)
make run-client
```

### Docker

```bash
# Build the Docker image
docker build -t dhashmap .

# Run the container
docker run -p 8080:8080 dhashmap
```

## API Documentation

API documentation can be found in the `docs` directory.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.