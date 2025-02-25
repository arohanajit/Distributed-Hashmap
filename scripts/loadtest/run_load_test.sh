#!/bin/bash

# Exit on any error
set -e

# Default settings
TARGET_URL="http://localhost:8080"
DURATION=60
THREADS=10
READ_RATIO=0.8
KEY_COUNT=1000
VALUE_SIZE=1024
OUTPUT_DIR="./results"
MONITOR_CONTAINERS="dhashmap-node1,dhashmap-node2,dhashmap-node3"
TEST_TYPE="throughput"
RPS=1000
MAX_KEYS=100000

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --url)
      TARGET_URL="$2"
      shift 2
      ;;
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --threads)
      THREADS="$2"
      shift 2
      ;;
    --read-ratio)
      READ_RATIO="$2"
      shift 2
      ;;
    --key-count)
      KEY_COUNT="$2"
      shift 2
      ;;
    --value-size)
      VALUE_SIZE="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --monitor-containers)
      MONITOR_CONTAINERS="$2"
      shift 2
      ;;
    --test-type)
      TEST_TYPE="$2"
      shift 2
      ;;
    --rps)
      RPS="$2"
      shift 2
      ;;
    --max-keys)
      MAX_KEYS="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --url URL                  Target URL (default: $TARGET_URL)"
      echo "  --duration SECONDS         Test duration in seconds (default: $DURATION)"
      echo "  --threads COUNT            Number of threads (default: $THREADS)"
      echo "  --read-ratio RATIO         Read ratio (0.0-1.0, default: $READ_RATIO)"
      echo "  --key-count COUNT          Number of keys (default: $KEY_COUNT)"
      echo "  --value-size BYTES         Value size in bytes (default: $VALUE_SIZE)"
      echo "  --output-dir DIR           Output directory (default: $OUTPUT_DIR)"
      echo "  --monitor-containers LIST  Comma-separated list of containers to monitor (default: $MONITOR_CONTAINERS)"
      echo "  --test-type TYPE           Test type: throughput or stress (default: $TEST_TYPE)"
      echo "  --rps COUNT                Requests per second (for throughput test, default: $RPS)"
      echo "  --max-keys COUNT           Maximum keys (for stress test, default: $MAX_KEYS)"
      echo "  --help                     Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Create output directory
mkdir -p "$OUTPUT_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TEST_OUTPUT_DIR="${OUTPUT_DIR}/${TEST_TYPE}_test_${TIMESTAMP}"
mkdir -p "$TEST_OUTPUT_DIR"

# Save test configuration
cat > "${TEST_OUTPUT_DIR}/config.json" << EOF
{
  "target_url": "${TARGET_URL}",
  "duration": ${DURATION},
  "threads": ${THREADS},
  "read_ratio": ${READ_RATIO},
  "key_count": ${KEY_COUNT},
  "value_size": ${VALUE_SIZE},
  "test_type": "${TEST_TYPE}",
  "rps": ${RPS},
  "max_keys": ${MAX_KEYS},
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

# Start system monitoring in the background
echo -e "${GREEN}Starting system monitoring...${NC}"
MONITOR_OUTPUT_DIR="${TEST_OUTPUT_DIR}/monitoring"
mkdir -p "$MONITOR_OUTPUT_DIR"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is not installed. Please install Python 3 and try again.${NC}"
    exit 1
fi

# Filter out containers that don't exist
EXISTING_CONTAINERS=""
IFS=',' read -ra CONTAINER_ARRAY <<< "$MONITOR_CONTAINERS"
for container in "${CONTAINER_ARRAY[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        if [ -z "$EXISTING_CONTAINERS" ]; then
            EXISTING_CONTAINERS="$container"
        else
            EXISTING_CONTAINERS="$EXISTING_CONTAINERS,$container"
        fi
    else
        echo -e "${YELLOW}Warning: Container '$container' not found and will be skipped for monitoring.${NC}"
    fi
done

# If no containers exist, provide a warning but continue
if [ -z "$EXISTING_CONTAINERS" ]; then
    echo -e "${YELLOW}Warning: None of the specified containers were found. System monitoring will only track host metrics.${NC}"
    echo -e "${YELLOW}If this is a test run without the actual distributed hashmap system running, this is normal.${NC}"
    # Use a placeholder to avoid errors in the monitoring script
    EXISTING_CONTAINERS="none"
else
    echo -e "${GREEN}Monitoring containers: $EXISTING_CONTAINERS${NC}"
fi

# Start the system monitor
echo -e "${YELLOW}Starting system monitor...${NC}"
python3 ../monitoring/system_monitor.py \
  --interval 1 \
  --output-dir "$MONITOR_OUTPUT_DIR" \
  --containers "$EXISTING_CONTAINERS" \
  --duration "$DURATION" &
MONITOR_PID=$!

# Wait a moment for the monitor to start
sleep 2

# Build the load test tool if needed
echo -e "${GREEN}Building load test tools...${NC}"
cd "$(dirname "$0")"
go build -o loadtest loadtest.go
go build -o stress_test stress_test.go

# Run the appropriate test
if [ "$TEST_TYPE" == "throughput" ]; then
    echo -e "${GREEN}Running throughput test...${NC}"
    ./loadtest \
      --target-url "$TARGET_URL" \
      --num-threads "$THREADS" \
      --duration "$DURATION" \
      --read-write-ratio "$READ_RATIO" \
      --key-count "$KEY_COUNT" \
      --value-size "$VALUE_SIZE" \
      --report-interval 5 \
      --output-file "${TEST_OUTPUT_DIR}/throughput_results.json" \
      --rps "$RPS"
elif [ "$TEST_TYPE" == "stress" ]; then
    echo -e "${GREEN}Running stress test...${NC}"
    ./stress_test \
      --target-url "$TARGET_URL" \
      --num-threads "$THREADS" \
      --duration "$DURATION" \
      --key-count "$KEY_COUNT" \
      --value-size "$VALUE_SIZE" \
      --report-interval 5 \
      --output-file "${TEST_OUTPUT_DIR}/stress_results.json" \
      --max-keys "$MAX_KEYS"
else
    echo -e "${RED}Error: Unknown test type: $TEST_TYPE${NC}"
    exit 1
fi

# Wait for the monitor to finish
if ps -p $MONITOR_PID > /dev/null; then
    echo -e "${YELLOW}Waiting for system monitor to finish...${NC}"
    wait $MONITOR_PID
fi

# Generate a summary report
echo -e "${GREEN}Generating test summary...${NC}"
cat > "${TEST_OUTPUT_DIR}/summary.md" << EOF
# Load Test Summary

## Test Configuration
- **Test Type**: $TEST_TYPE
- **Target URL**: $TARGET_URL
- **Duration**: $DURATION seconds
- **Threads**: $THREADS
- **Read Ratio**: $READ_RATIO
- **Key Count**: $KEY_COUNT
- **Value Size**: $VALUE_SIZE bytes
- **Timestamp**: $(date -u +"%Y-%m-%d %H:%M:%S UTC")

## Results
The detailed results can be found in the following files:
- Test results: \`${TEST_TYPE}_results.json\`
- System monitoring data: \`monitoring/\`

## Next Steps
1. Analyze the results using the provided data
2. Compare with baseline metrics
3. Identify bottlenecks and areas for optimization
EOF

echo -e "${GREEN}Test completed successfully!${NC}"
echo -e "Results saved to: ${TEST_OUTPUT_DIR}"
echo -e "Summary report: ${TEST_OUTPUT_DIR}/summary.md" 