# Integration and Chaos Testing

This directory contains integration and chaos tests for the Distributed Hashmap system.

## Test Structure

- `integration/`: Contains integration tests for multi-node scenarios
  - `cluster_test.go`: Tests for data consistency, replication, and node failure/recovery
  
- `chaos/`: Contains chaos tests for failure simulations
  - `network_partition_test.go`: Tests for network partitions, node failures, and latency

## Running Tests

### Prerequisites

- Docker
- Docker Compose
- Go 1.21 or later

### Running Integration Tests

```bash
make test-integration
```

This will:
1. Build Docker images for the server and test runner
2. Start a 5-node cluster with etcd for service discovery
3. Run the integration tests
4. Clean up the test environment

### Running Chaos Tests

```bash
make test-chaos
```

This will:
1. Build Docker images for the server and test runner
2. Start a 5-node cluster with etcd for service discovery
3. Run the chaos tests, which include:
   - Network partition tests
   - Node failure and recovery tests
   - Random node termination tests
   - Network latency tests
4. Clean up the test environment

### Running All Tests

```bash
make test-all
```

This will run unit tests, integration tests, and chaos tests.

## Test Scenarios

### Integration Tests

- **Basic Data Consistency**: Write to one node, read from another
- **Multiple Writes**: Write different key-value pairs to different nodes
- **Node Failure and Recovery**: Simulate node failure and verify data availability
- **Idempotency**: Test that repeated operations are idempotent

### Chaos Tests

- **Network Partition**: Simulate split-brain scenario by creating network partitions
- **Node Failure and Recovery**: Kill nodes mid-operation and verify recovery
- **Random Node Termination**: Randomly terminate and restart nodes
- **Network Latency**: Introduce latency between nodes

## Adding New Tests

To add new integration tests:
1. Add test functions to `integration/cluster_test.go` or create a new test file
2. Ensure the test file has the `// +build integration` build tag

To add new chaos tests:
1. Add test functions to `chaos/network_partition_test.go` or create a new test file
2. Ensure the test file has the `// +build chaos` build tag 