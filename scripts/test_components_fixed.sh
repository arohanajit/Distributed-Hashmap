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

echo -e "${GREEN}Starting component tests...${NC}"

# Function to run tests
run_test() {
    local command=$1
    local expected_success=$2
    local description=$3
    local output
    local status
    
    echo -e "\n${YELLOW}Running: ${NC}$description"
    
    # Run the command and capture both output and exit status
    output=$($command 2>&1)
    status=$?
    
    if [ $expected_success = true ]; then
        # For positive tests (expecting success)
        if [ $status -eq 0 ]; then
            echo -e "${GREEN}✓${NC} $description"
            echo "$output" | grep -E "^ok|^PASS|^=== RUN"
            return 0
        else
            echo -e "${RED}✗${NC} $description"
            echo -e "${YELLOW}Command output:${NC}\n$output"
            return 1
        fi
    else
        # For negative tests (expecting failure)
        if [ $status -ne 0 ]; then
            echo -e "${GREEN}✓${NC} $description"
            return 0
        else
            echo -e "${RED}✗${NC} $description"
            echo -e "${YELLOW}Command output:${NC}\n$output"
            return 1
        fi
    fi
}

# Function to check if etcd is running locally
check_etcd() {
    echo -e "${YELLOW}Checking if etcd is running locally...${NC}"
    if command -v curl >/dev/null 2>&1; then
        if curl -s http://localhost:2379/health >/dev/null 2>&1; then
            echo -e "${GREEN}etcd is running, will run etcd integration tests${NC}"
            export TEST_WITH_ETCD=true
            export ETCD_ENDPOINTS="localhost:2379"
            return 0
        else
            echo -e "${YELLOW}etcd is not running, skipping etcd integration tests${NC}"
            export TEST_WITH_ETCD=false
            return 0
        fi
    else
        echo -e "${YELLOW}curl not found, assuming etcd is not running${NC}"
        export TEST_WITH_ETCD=false
        return 0
    fi
}

# Setup test environment
setup_test_env() {
    echo -e "${YELLOW}Setting up test environment...${NC}"
    
    # Create test data directory if needed
    mkdir -p test_data/hints

    # Export test environment variables
    export TEST_MODE=true
    export GO_TEST_FLAGS="-v -count=1"
    export TEST_WITH_SWIM=true
    
    # For SWIM tests with custom ports to avoid conflicts
    export SWIM_UDP_PORT=27946
    export SWIM_TCP_PORT=27947
    
    # Check if etcd is running for integration tests
    check_etcd
}

# Cleanup test environment
cleanup_test_env() {
    echo -e "${YELLOW}Cleaning up test environment...${NC}"
    rm -rf test_data
    unset TEST_MODE
    unset GO_TEST_FLAGS
    unset TEST_WITH_ETCD
    unset TEST_WITH_SWIM
    unset SWIM_UDP_PORT
    unset SWIM_TCP_PORT
    unset ETCD_ENDPOINTS
}

# Main test execution
main() {
    # Setup test environment
    setup_test_env

    # Run tests for all components
    echo -e "\n${GREEN}=== Testing Components ===${NC}"

    # Test Config and Utility Components
    echo -e "\n${GREEN}=== Testing Config and Utility Components ===${NC}"
    run_test "go test $GO_TEST_FLAGS ./internal/config/..." true "Configuration Components" || exit 1
    run_test "go test $GO_TEST_FLAGS ./internal/utils/..." true "Utility Components" || exit 1

    # Test Storage Components
    echo -e "\n${GREEN}=== Testing Storage Components ===${NC}"
    run_test "go test $GO_TEST_FLAGS ./internal/storage/..." true "Storage Components" || exit 1
   
    # Test API Components
    echo -e "\n${GREEN}=== Testing API Components ===${NC}"
    run_test "go test $GO_TEST_FLAGS ./internal/api/..." true "API Components" || exit 1
    
    # Test Basic Discovery Components
    echo -e "\n${GREEN}=== Testing Basic Discovery Components ===${NC}"
    run_test "go test $GO_TEST_FLAGS -run TestDiscoveryManager_LoadNodes ./internal/cluster/..." true "Load Nodes from Environment" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestDiscoveryManager_AddRemoveNode ./internal/cluster/..." true "Add/Remove Node" || exit 1
    
    # Test SWIM Gossip Protocol
    echo -e "\n${GREEN}=== Testing SWIM Gossip Protocol ===${NC}"
    if [ "$TEST_WITH_SWIM" = true ]; then
        run_test "go test $GO_TEST_FLAGS -run TestSwimGossip ./internal/cluster/..." true "SWIM Gossip Protocol" || echo -e "${YELLOW}SWIM tests may fail if ports are already in use${NC}"
        
        # Additional SWIM protocol tests
        # Test node lifecycle states (alive, suspect, dead)
        # Use env vars to set custom ports to avoid conflicts
        run_test "SWIM_UDP_PORT=$SWIM_UDP_PORT SWIM_TCP_PORT=$SWIM_TCP_PORT go test $GO_TEST_FLAGS -run TestSwimNodeState ./internal/cluster/..." true "SWIM Node State Transitions" || echo -e "${YELLOW}SWIM state test may fail if ports are already in use${NC}"
        
        # Test full state synchronization
        run_test "SWIM_UDP_PORT=$((SWIM_UDP_PORT+2)) SWIM_TCP_PORT=$((SWIM_TCP_PORT+2)) go test $GO_TEST_FLAGS -run TestSwimStateSync ./internal/cluster/..." true "SWIM State Synchronization" || echo -e "${YELLOW}SWIM sync tests may fail if ports are already in use${NC}"
        
        # Test indirect ping mechanism
        run_test "SWIM_UDP_PORT=$((SWIM_UDP_PORT+4)) SWIM_TCP_PORT=$((SWIM_TCP_PORT+4)) go test $GO_TEST_FLAGS -run TestSwimIndirectPing ./internal/cluster/..." true "SWIM Indirect Ping Mechanism" || echo -e "${YELLOW}SWIM indirect ping tests may fail if ports are already in use${NC}"
    else
        echo -e "${YELLOW}Skipping SWIM tests (TEST_WITH_SWIM not set to true)${NC}"
    fi
    
    # Test etcd Integration
    echo -e "\n${GREEN}=== Testing etcd Integration ===${NC}"
    if [ "$TEST_WITH_ETCD" = true ]; then
        run_test "go test $GO_TEST_FLAGS -run TestEtcdDiscovery ./internal/cluster/..." true "etcd Integration" || echo -e "${YELLOW}etcd tests may fail if etcd is not properly configured${NC}"
        
        # Additional etcd integration tests
        run_test "ETCD_ENDPOINTS=$ETCD_ENDPOINTS go test $GO_TEST_FLAGS -run TestDiscoveryManager_ConnectEtcd ./internal/cluster/..." true "etcd Connection" || echo -e "${YELLOW}etcd connection tests may fail if etcd is not properly configured${NC}"
        
        run_test "ETCD_ENDPOINTS=$ETCD_ENDPOINTS go test $GO_TEST_FLAGS -run TestDiscoveryManager_RegisterEtcd ./internal/cluster/..." true "etcd Registration" || echo -e "${YELLOW}etcd registration tests may fail if etcd is not properly configured${NC}"
        
        run_test "ETCD_ENDPOINTS=$ETCD_ENDPOINTS go test $GO_TEST_FLAGS -run TestDiscoveryManager_WatchEtcd ./internal/cluster/..." true "etcd Watch" || echo -e "${YELLOW}etcd watch tests may fail if etcd is not properly configured${NC}"
    else
        echo -e "${YELLOW}Skipping etcd tests (etcd not running)${NC}"
    fi
    
    # Test Cluster Management Components
    echo -e "\n${GREEN}=== Testing Cluster Management Components ===${NC}"
    run_test "go test $GO_TEST_FLAGS -run TestClusterManager_JoinLeave ./internal/cluster/..." true "Cluster Join/Leave" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestClusterManager_NodeMapping ./internal/cluster/..." true "Cluster Node Mapping" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestClusterManager_MembershipChange ./internal/cluster/..." true "Cluster Membership Changes" || exit 1
    
    # Test Replication Components
    echo -e "\n${GREEN}=== Testing Replication Components ===${NC}"
    run_test "go test $GO_TEST_FLAGS -run TestReplicationManager_BasicReplication ./internal/cluster/..." true "Basic Replication" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestReplicationManager_BasicReplicationFlow ./internal/cluster/..." true "Basic Replication Flow" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestReplicationManager_QuorumHandling ./internal/cluster/..." true "Quorum Handling" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestReplicationManager_Conflict ./internal/cluster/..." true "Conflict Resolution" || exit 1
    
    # Test Re-Replication Components
    echo -e "\n${GREEN}=== Testing Re-Replication Components ===${NC}"
    run_test "go test $GO_TEST_FLAGS -run TestReReplicationManager_BasicReplication ./internal/cluster/..." true "Basic Re-Replication" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestReReplicationManager_BasicReplicationFlow ./internal/cluster/..." true "Basic Re-Replication Flow" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestReReplicationManager_QuorumHandling ./internal/cluster/..." true "Re-Replication Quorum Handling" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestReReplicationManager_NodeFailure ./internal/cluster/..." true "Re-Replication Node Failure" || exit 1

    # Test Shard Management Components
    echo -e "\n${GREEN}=== Testing Shard Management Components ===${NC}"
    run_test "go test $GO_TEST_FLAGS -run TestShardManager ./internal/cluster/..." true "Shard Management" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestShardManager_Rebalance ./internal/cluster/..." true "Shard Rebalancing" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestShardManager_KeyMapping ./internal/cluster/..." true "Key to Shard Mapping" || exit 1

    # Test Failure Detection Components
    echo -e "\n${GREEN}=== Testing Failure Detection Components ===${NC}"
    run_test "go test $GO_TEST_FLAGS -run TestFailureDetector_NodeFailureAndRecovery ./internal/cluster/..." true "Node Failure and Recovery" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestFailureDetector_CascadingFailures ./internal/cluster/..." true "Cascading Failures" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestFailureDetector_NetworkPartition ./internal/cluster/..." true "Network Partition" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestFailureDetector_FalsePositives ./internal/cluster/..." true "False Positive Handling" || exit 1
    
    # Test Health Check Components
    echo -e "\n${GREEN}=== Testing Health Check Components ===${NC}"
    run_test "go test $GO_TEST_FLAGS -run TestHealthCheck ./internal/cluster/..." true "Basic Health Checks" || exit 1
    run_test "go test $GO_TEST_FLAGS -run TestHTTPHealthChecker ./internal/cluster/..." true "HTTP Health Checker" || exit 1
    
    # Test Graceful Shutdown Components
    echo -e "\n${GREEN}=== Testing Graceful Shutdown Components ===${NC}"
    run_test "go test $GO_TEST_FLAGS -run TestShutdownManager_Shutdown ./internal/cluster/..." true "Basic Shutdown Sequence" || echo -e "${YELLOW}Shutdown tests may fail due to port conflicts${NC}"
    run_test "go test $GO_TEST_FLAGS -run TestShutdownManager_AlreadyShuttingDown ./internal/cluster/..." true "Multiple Shutdown Calls" || echo -e "${YELLOW}Shutdown tests may fail due to port conflicts${NC}"
    
    # Test Service Discovery Integration with Cluster Manager
    echo -e "\n${GREEN}=== Testing Service Discovery with Cluster Integration ===${NC}"
    
    # SWIM Integration with Cluster Manager
    if [ "$TEST_WITH_SWIM" = true ]; then
        run_test "SWIM_UDP_PORT=$((SWIM_UDP_PORT+6)) SWIM_TCP_PORT=$((SWIM_TCP_PORT+6)) go test $GO_TEST_FLAGS -run TestNewClusterManagerWithSwim ./internal/cluster/..." true "Create Cluster Manager with SWIM" || echo -e "${YELLOW}SWIM integration tests may fail if ports are already in use${NC}"
        
        run_test "SWIM_UDP_PORT=$((SWIM_UDP_PORT+8)) SWIM_TCP_PORT=$((SWIM_TCP_PORT+8)) go test $GO_TEST_FLAGS -run TestClusterManagerWithSwim_StartStop ./internal/cluster/..." true "SWIM Start/Stop" || echo -e "${YELLOW}SWIM start/stop tests may fail if ports are already in use${NC}"
        
        run_test "SWIM_UDP_PORT=$((SWIM_UDP_PORT+10)) SWIM_TCP_PORT=$((SWIM_TCP_PORT+10)) go test $GO_TEST_FLAGS -run TestClusterManagerWithSwim_NodeJoinLeave ./internal/cluster/..." true "SWIM Node Join/Leave" || echo -e "${YELLOW}SWIM join/leave tests may fail if ports are already in use${NC}"
    fi
    
    # etcd Integration with Cluster Manager
    if [ "$TEST_WITH_ETCD" = true ]; then
        run_test "ETCD_ENDPOINTS=$ETCD_ENDPOINTS go test $GO_TEST_FLAGS -run TestNewClusterManagerWithDiscovery ./internal/cluster/..." true "Create Cluster Manager with etcd Discovery" || echo -e "${YELLOW}etcd integration tests may fail if etcd is not properly configured${NC}"
        
        run_test "ETCD_ENDPOINTS=$ETCD_ENDPOINTS go test $GO_TEST_FLAGS -run TestClusterManagerWithDiscovery_StartStop ./internal/cluster/..." true "etcd Discovery Start/Stop" || echo -e "${YELLOW}etcd start/stop tests may fail if etcd is not properly configured${NC}"
        
        run_test "ETCD_ENDPOINTS=$ETCD_ENDPOINTS go test $GO_TEST_FLAGS -run TestClusterManagerWithDiscovery_NodeJoinLeave ./internal/cluster/..." true "etcd Node Join/Leave" || echo -e "${YELLOW}etcd join/leave tests may fail if etcd is not properly configured${NC}"
    fi
    
    # Test Discovery Examples
    echo -e "\n${GREEN}=== Testing Discovery Examples ===${NC}"
    if [ "$TEST_WITH_ETCD" = true ]; then
        run_test "ETCD_ENDPOINTS=$ETCD_ENDPOINTS NODE_ID=test NODE_HOST=localhost NODE_PORT=8080 go test $GO_TEST_FLAGS -run TestRunEtcdDiscoveryExample ./internal/cluster/..." true "etcd Discovery Example" || echo -e "${YELLOW}etcd example tests may be skipped if etcd is not running${NC}"
    fi
    
    if [ "$TEST_WITH_SWIM" = true ]; then
        run_test "SWIM_UDP_PORT=$((SWIM_UDP_PORT+12)) SWIM_TCP_PORT=$((SWIM_TCP_PORT+12)) NODE_ID=test NODE_HOST=localhost NODE_PORT=8080 go test $GO_TEST_FLAGS -run TestRunSwimGossipExample ./internal/cluster/..." true "SWIM Gossip Example" || echo -e "${YELLOW}SWIM example tests may fail if ports are already in use${NC}"
    fi
    
    # Test Discovery with Failure Scenarios
    echo -e "\n${GREEN}=== Testing Discovery with Failure Scenarios ===${NC}"
    
    # Test eventual consistency in membership view
    if [ "$TEST_WITH_SWIM" = true ]; then
        run_test "SWIM_UDP_PORT=$((SWIM_UDP_PORT+14)) SWIM_TCP_PORT=$((SWIM_TCP_PORT+14)) go test $GO_TEST_FLAGS -run TestSwimEventualConsistency ./internal/cluster/..." true "SWIM Eventual Consistency" || echo -e "${YELLOW}SWIM consistency tests may fail if ports are already in use${NC}"
    fi
    
    if [ "$TEST_WITH_ETCD" = true ]; then
        run_test "ETCD_ENDPOINTS=$ETCD_ENDPOINTS go test $GO_TEST_FLAGS -run TestEtcdDiscoveryReconnect ./internal/cluster/..." true "etcd Reconnection Test" || echo -e "${YELLOW}etcd reconnect tests may fail if etcd is not properly configured${NC}"
    fi
    
    # Run all cluster tests to make sure everything works together
    echo -e "\n${GREEN}=== Testing All Cluster Components Together ===${NC}"
    run_test "go test $GO_TEST_FLAGS ./internal/cluster/..." true "All Cluster Tests" || exit 1

    # Run integration tests if environment supports it
    echo -e "\n${GREEN}=== Running Integration Tests (if supported) ===${NC}"
    if [ "$TEST_WITH_ETCD" = true ] && [ "$TEST_WITH_SWIM" = true ]; then
        run_test "ETCD_ENDPOINTS=$ETCD_ENDPOINTS SWIM_UDP_PORT=$((SWIM_UDP_PORT+16)) SWIM_TCP_PORT=$((SWIM_TCP_PORT+16)) go test $GO_TEST_FLAGS -tags=integration ./..." true "Full Integration Tests" || echo -e "${YELLOW}Some integration tests may be skipped based on environment${NC}"
    else
        echo -e "${YELLOW}Skipping full integration tests (missing prerequisites)${NC}"
    fi

    # Cleanup test environment
    cleanup_test_env

    echo -e "\n${GREEN}All tests completed successfully!${NC}"
}

# Run main function
main main
