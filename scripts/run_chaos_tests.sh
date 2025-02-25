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

echo -e "${GREEN}Starting chaos tests...${NC}"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running. Please start Docker and try again.${NC}"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker Compose is not installed. Please install Docker Compose and try again.${NC}"
    exit 1
fi

# Check if iptables is available in the test container
if ! docker run --rm dhashmap-test which iptables > /dev/null 2>&1; then
    echo -e "${RED}Error: iptables is not available in the test container. Please install iptables in the Dockerfile.test.${NC}"
    exit 1
fi

# Build the Docker images
echo -e "${YELLOW}Building Docker images...${NC}"
docker build -t dhashmap-server .
docker build -t dhashmap-test -f Dockerfile.test .

# Start the test cluster
echo -e "${YELLOW}Starting test cluster...${NC}"
docker-compose -f docker-compose.test.yml up -d etcd node1 node2 node3 node4 node5

# Wait for the cluster to start
echo -e "${YELLOW}Waiting for the cluster to start...${NC}"
sleep 30

# Run the chaos tests
echo -e "${YELLOW}Running chaos tests...${NC}"
docker-compose -f docker-compose.test.yml run --rm test-runner go test -v -tags=chaos ./tests/chaos/...

# Get the exit code of the test runner
TEST_EXIT_CODE=$?

# Stop and remove the test cluster
echo -e "${YELLOW}Stopping test cluster...${NC}"
docker-compose -f docker-compose.test.yml down -v

# Check if the tests passed
if [ "$TEST_EXIT_CODE" -eq 0 ]; then
    echo -e "${GREEN}Chaos tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Chaos tests failed!${NC}"
    exit 1
fi 