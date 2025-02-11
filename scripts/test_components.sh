#!/bin/bash

# Exit on any error
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Change to project root directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$PROJECT_ROOT"

echo "Starting component tests..."

# Function to run tests
run_test() {
    local output
    local status
    
    # Run the command and capture both output and exit status
    output=$($1 2>&1)
    status=$?
    
    if [ $2 = true ]; then
        # For positive tests (expecting success)
        if [ $status -eq 0 ]; then
            echo -e "${GREEN}✓${NC} $3"
            echo "$output" | grep -E "^ok|^PASS|^=== RUN"
            return 0
        else
            echo -e "${RED}✗${NC} $3"
            echo -e "${YELLOW}Command output:${NC}\n$output"
            return 1
        fi
    else
        # For negative tests (expecting failure)
        if [ $status -ne 0 ]; then
            echo -e "${GREEN}✓${NC} $3"
            return 0
        else
            echo -e "${RED}✗${NC} $3"
            echo -e "${YELLOW}Command output:${NC}\n$output"
            return 1
        fi
    fi
}

# Setup test environment
setup_test_env() {
    echo "Setting up test environment..."
    
    # Create test config if needed
    mkdir -p config
    cat > config/test.yaml << EOF
storage:
  data_dir: "./test_data"
  max_size_mb: 100
  hints_dir: "./test_data/hints"
cluster:
  node_id: "test-node-1"
  nodes:
    - id: "test-node-1"
      address: "localhost:8080"
    - id: "test-node-2"
      address: "localhost:8081"
replication:
  factor: 3
  write_quorum: 2
  heartbeat_interval: "5s"
EOF

    # Create test data directory and hints directory
    mkdir -p test_data/hints

    # Export test environment variables
    export TEST_MODE=true
    export CONFIG_FILE="config/test.yaml"
    export GO_TEST_FLAGS="-v -count=1"
}

# Cleanup test environment
cleanup_test_env() {
    echo "Cleaning up test environment..."
    rm -rf test_data
    rm -f config/test.yaml
    unset TEST_MODE
    unset CONFIG_FILE
    unset GO_TEST_FLAGS
}

# Run unit tests for a package
run_unit_tests() {
    local package=$1
    echo "Running unit tests for $package..."
    go test $GO_TEST_FLAGS ./$package/... || return 1
}

# Run integration tests for a package
run_integration_tests() {
    local package=$1
    echo "Running integration tests for $package..."
    go test $GO_TEST_FLAGS -tags=integration ./$package/... || return 1
}

# Main test execution
main() {
    # Setup test environment
    setup_test_env

    # Run unit tests
    echo -e "\n=== Running Unit Tests ==="
    
    echo -e "\n=== Testing Configuration and Logging ==="
    run_unit_tests "internal/config" || exit 1
    
    echo -e "\n=== Testing REST API Components ==="
    run_unit_tests "internal/api/rest" || exit 1
    
    echo -e "\n=== Testing Storage Layer ==="
    run_unit_tests "internal/storage" || exit 1
    
    echo -e "\n=== Testing Replication Components ==="
    echo "Testing basic replication..."
    go test $GO_TEST_FLAGS -run "TestReplicator_BasicReplication" ./internal/cluster/... || exit 1
    echo "Testing quorum compliance..."
    go test $GO_TEST_FLAGS -run "TestReplicator_QuorumCompliance" ./internal/cluster/... || exit 1
    echo "Testing re-replication..."
    go test $GO_TEST_FLAGS -run "TestReplicator_ReReplication" ./internal/cluster/... || exit 1
    
    echo -e "\n=== Testing Failure Detection Components ==="
    echo "Testing node failure and recovery..."
    go test $GO_TEST_FLAGS -run "TestFailureDetector_NodeFailureAndRecovery" ./internal/cluster/... || exit 1
    echo "Testing cascading failures..."
    go test $GO_TEST_FLAGS -run "TestFailureDetector_CascadingFailures" ./internal/cluster/... || exit 1
    echo "Testing network partitions..."
    go test $GO_TEST_FLAGS -run "TestFailureDetector_NetworkPartition" ./internal/cluster/... || exit 1
    
    echo -e "\n=== Testing Hinted Handoff Components ==="
    echo "Testing node failure and recovery..."
    go test $GO_TEST_FLAGS -run "TestHintedHandoff_NodeFailureAndRecovery" ./internal/storage/... || exit 1
    echo "Testing batch processing..."
    go test $GO_TEST_FLAGS -run "TestHintedHandoff_BatchProcessing" ./internal/storage/... || exit 1
    echo "Testing expired hints..."
    go test $GO_TEST_FLAGS -run "TestHintedHandoff_ExpiredHints" ./internal/storage/... || exit 1
    
    echo -e "\n=== Testing Cluster Components ==="
    run_unit_tests "internal/cluster" || exit 1

    # Run integration tests if not in CI
    if [ -z "$CI" ]; then
        echo -e "\n=== Running Integration Tests ==="
        
        # Kill any existing server process
        echo "Cleaning up existing server processes..."
        pkill -f "server/main" || true
        sleep 1

        # Start test servers
        echo "Starting test servers..."
        go run cmd/server/main.go -config config/test.yaml &
        SERVER1_PID=$!
        
        # Wait for servers to start
        sleep 2

        # Run integration tests
        run_integration_tests "internal/api/rest" || {
            kill $SERVER1_PID
            exit 1
        }

        # Cleanup
        kill $SERVER1_PID || true
    fi

    # Cleanup test environment
    cleanup_test_env

    echo -e "\n${GREEN}All tests completed successfully!${NC}"
}

# Run main function
main 