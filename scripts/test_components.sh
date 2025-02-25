#!/bin/bash

# Exit on any error
set -e

# Default settings
TARGET_URL="http://localhost:8080"
TEST_TYPE="throughput"
DURATION=60
THREADS=10
READ_RATIO=0.8
KEY_COUNT=1000
VALUE_SIZE=1024
RPS=1000
MAX_KEYS=100000
MONITOR_CONTAINERS="dhashmap-node1,dhashmap-node2,dhashmap-node3"
COMPARE_WITH=""
SKIP_MONITORING=false
SKIP_ANALYSIS=false
OUTPUT_DIR="./results"
SKIP_PYTHON_DEPS=false
USE_VENV=true

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VENV_DIR="$SCRIPT_DIR/.venv"

# Function to display usage information
function show_usage {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --url URL                  Target URL (default: $TARGET_URL)"
    echo "  --test-type TYPE           Test type: throughput, stress, or all (default: $TEST_TYPE)"
    echo "  --duration SECONDS         Test duration in seconds (default: $DURATION)"
    echo "  --threads COUNT            Number of threads (default: $THREADS)"
    echo "  --read-ratio RATIO         Read ratio (0.0-1.0, default: $READ_RATIO)"
    echo "  --key-count COUNT          Number of keys (default: $KEY_COUNT)"
    echo "  --value-size BYTES         Value size in bytes (default: $VALUE_SIZE)"
    echo "  --rps COUNT                Requests per second (for throughput test, default: $RPS)"
    echo "  --max-keys COUNT           Maximum keys (for stress test, default: $MAX_KEYS)"
    echo "  --monitor-containers LIST  Comma-separated list of containers to monitor (default: $MONITOR_CONTAINERS)"
    echo "  --output-dir DIR           Output directory (default: $OUTPUT_DIR)"
    echo "  --compare-with DIR         Compare results with a previous test"
    echo "  --skip-monitoring          Skip starting the monitoring stack"
    echo "  --skip-analysis            Skip analyzing the results"
    echo "  --skip-python-deps         Skip Python dependency installation"
    echo "  --no-venv                  Don't use a Python virtual environment"
    echo "  --help                     Show this help message"
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --url)
      TARGET_URL="$2"
      shift 2
      ;;
    --test-type)
      TEST_TYPE="$2"
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
    --rps)
      RPS="$2"
      shift 2
      ;;
    --max-keys)
      MAX_KEYS="$2"
      shift 2
      ;;
    --monitor-containers)
      MONITOR_CONTAINERS="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --compare-with)
      COMPARE_WITH="$2"
      shift 2
      ;;
    --skip-monitoring)
      SKIP_MONITORING=true
      shift
      ;;
    --skip-analysis)
      SKIP_ANALYSIS=true
      shift
      ;;
    --skip-python-deps)
      SKIP_PYTHON_DEPS=true
      shift
      ;;
    --no-venv)
      USE_VENV=false
      shift
      ;;
    --help)
      show_usage
      ;;
    *)
      echo "Unknown option: $1"
      show_usage
      ;;
  esac
done

# Validate test type
if [[ "$TEST_TYPE" != "throughput" && "$TEST_TYPE" != "stress" && "$TEST_TYPE" != "all" ]]; then
    echo -e "${RED}Error: Invalid test type. Must be 'throughput', 'stress', or 'all'.${NC}"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to create and activate a Python virtual environment
function setup_venv {
    if [ "$USE_VENV" = false ]; then
        echo -e "${YELLOW}Skipping virtual environment setup as requested.${NC}"
        return 0
    fi
    
    echo -e "${BLUE}Setting up Python virtual environment...${NC}"
    
    # Check if venv module is available
    if ! python3 -m venv --help &> /dev/null; then
        echo -e "${RED}Error: Python venv module is not available.${NC}"
        echo -e "${YELLOW}Please install it or use --no-venv to skip virtual environment setup.${NC}"
        return 1
    fi
    
    # Create virtual environment if it doesn't exist
    if [ ! -d "$VENV_DIR" ]; then
        echo -e "${YELLOW}Creating new virtual environment in $VENV_DIR...${NC}"
        python3 -m venv "$VENV_DIR"
    fi
    
    # Activate virtual environment
    echo -e "${YELLOW}Activating virtual environment...${NC}"
    source "$VENV_DIR/bin/activate"
    
    # Verify activation
    if [[ "$VIRTUAL_ENV" != "$VENV_DIR" ]]; then
        echo -e "${RED}Error: Failed to activate virtual environment.${NC}"
        return 1
    fi
    
    echo -e "${GREEN}Virtual environment is active.${NC}"
    return 0
}

# Function to install Python packages
function install_python_packages {
    if [ "$SKIP_PYTHON_DEPS" = true ]; then
        echo -e "${YELLOW}Skipping Python dependency installation as requested.${NC}"
        return 0
    fi
    
    local requirements_file="$1"
    
    # Set up virtual environment if requested
    if [ "$USE_VENV" = true ]; then
        setup_venv || {
            echo -e "${YELLOW}Failed to set up virtual environment. Trying system installation...${NC}"
            USE_VENV=false
        }
    fi
    
    # If we're in a virtual environment, use its pip
    if [ -n "$VIRTUAL_ENV" ]; then
        echo -e "${YELLOW}Installing Python packages in virtual environment...${NC}"
        "$VIRTUAL_ENV/bin/pip" install -r "$requirements_file"
        return $?
    fi
    
    # Otherwise try system methods
    # Try pip3 first (most common on systems with Python 3)
    if command -v pip3 &> /dev/null; then
        echo -e "${YELLOW}Installing Python packages using pip3...${NC}"
        pip3 install --user -r "$requirements_file" || pip3 install --break-system-packages -r "$requirements_file"
        return $?
    fi
    
    # Try pip next
    if command -v pip &> /dev/null; then
        echo -e "${YELLOW}Installing Python packages using pip...${NC}"
        pip install --user -r "$requirements_file" || pip install --break-system-packages -r "$requirements_file"
        return $?
    fi
    
    # Try python3 -m pip
    if command -v python3 &> /dev/null; then
        echo -e "${YELLOW}Installing Python packages using python3 -m pip...${NC}"
        python3 -m pip install --user -r "$requirements_file" || python3 -m pip install --break-system-packages -r "$requirements_file"
        return $?
    fi
    
    # Try python -m pip
    if command -v python &> /dev/null; then
        echo -e "${YELLOW}Installing Python packages using python -m pip...${NC}"
        python -m pip install --user -r "$requirements_file" || python -m pip install --break-system-packages -r "$requirements_file"
        return $?
    fi
    
    # If we get here, we couldn't install packages
    echo -e "${RED}Error: Could not find pip or an alternative method to install Python packages.${NC}"
    echo -e "${YELLOW}Please install the following Python packages manually:${NC}"
    cat "$requirements_file"
    echo -e "${YELLOW}You can install them using: pip install -r $requirements_file${NC}"
    
    # Ask if the user wants to continue anyway
    read -p "Continue without installing Python packages? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
    
    return 1
}

# Function to check if required tools are installed
function check_prerequisites {
    echo -e "${BLUE}Checking prerequisites...${NC}"
    
    # Check if Go is installed
    if ! command -v go &> /dev/null; then
        echo -e "${RED}Error: Go is not installed. Please install Go and try again.${NC}"
        exit 1
    fi
    
    # Check if Python is installed
    if ! command -v python3 &> /dev/null; then
        if ! command -v python &> /dev/null; then
            echo -e "${RED}Error: Python is not installed. Please install Python 3 and try again.${NC}"
            exit 1
        else
            # Check if Python is version 3
            python_version=$(python --version 2>&1)
            if [[ ! $python_version =~ Python\ 3 ]]; then
                echo -e "${RED}Error: Python 3 is required. You have $python_version.${NC}"
                exit 1
            fi
        fi
    fi
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Error: Docker is not installed. Please install Docker and try again.${NC}"
        exit 1
    fi
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        echo -e "${RED}Error: Docker is not running. Please start Docker and try again.${NC}"
        exit 1
    fi
    
    # Check if Docker Compose is installed
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}Error: Docker Compose is not installed. Please install Docker Compose and try again.${NC}"
        exit 1
    fi
    
    # Check if required Python packages are installed
    if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
        install_python_packages "$SCRIPT_DIR/requirements.txt"
    else
        echo -e "${YELLOW}Warning: requirements.txt not found at $SCRIPT_DIR/requirements.txt${NC}"
        echo -e "${YELLOW}Skipping Python package installation.${NC}"
    fi
    
    echo -e "${GREEN}All prerequisites are satisfied.${NC}"
}

# Function to start the monitoring stack
function start_monitoring {
    if [ "$SKIP_MONITORING" = true ]; then
        echo -e "${YELLOW}Skipping monitoring stack startup as requested.${NC}"
        return
    fi
    
    echo -e "${BLUE}Starting monitoring stack...${NC}"
    
    # Make the script executable if it's not already
    chmod +x "$SCRIPT_DIR/monitoring/start_monitoring.sh"
    
    # Start the monitoring stack
    cd "$SCRIPT_DIR/monitoring"
    ./start_monitoring.sh
    
    echo -e "${GREEN}Monitoring stack started successfully.${NC}"
    echo -e "Prometheus: http://localhost:9090"
    echo -e "Grafana: http://localhost:3000 (admin/admin)"
    echo -e "Node Exporter: http://localhost:9100/metrics"
    echo -e "cAdvisor: http://localhost:8080"
}

# Function to analyze test results
function analyze_results {
    local test_dir=$1
    
    if [ "$SKIP_ANALYSIS" = true ]; then
        echo -e "${YELLOW}Skipping results analysis as requested.${NC}"
        return
    fi
    
    echo -e "${BLUE}Analyzing test results...${NC}"
    
    # Make the script executable if it's not already
    chmod +x "$SCRIPT_DIR/loadtest/analyze_results.py"
    
    # Run the analysis script
    cd "$SCRIPT_DIR/loadtest"
    
    # If we're using a virtual environment, make sure it's activated
    if [ "$USE_VENV" = true ] && [ -d "$VENV_DIR" ]; then
        source "$VENV_DIR/bin/activate"
    fi
    
    if [ -n "$COMPARE_WITH" ]; then
        ./analyze_results.py --results-dir "$test_dir" --compare-with "$COMPARE_WITH"
    else
        ./analyze_results.py --results-dir "$test_dir"
    fi
    
    echo -e "${GREEN}Analysis completed successfully.${NC}"
    echo -e "Analysis results saved to: ${test_dir}/analysis"
}

# Function to stop the monitoring stack
function stop_monitoring {
    if [ "$SKIP_MONITORING" = true ]; then
        return
    fi
    
    echo -e "${BLUE}Stopping monitoring stack...${NC}"
    
    cd "$SCRIPT_DIR/monitoring"
    docker-compose -f docker-compose.monitoring.yml down
    
    echo -e "${GREEN}Monitoring stack stopped successfully.${NC}"
}

# Function to clean up
function cleanup {
    # Deactivate virtual environment if it was activated
    if [ -n "$VIRTUAL_ENV" ]; then
        deactivate 2>/dev/null || true
    fi
}

# Set up trap to ensure cleanup happens
trap cleanup EXIT

# Main function to run the tests
function run_tests {
    # Check prerequisites
    check_prerequisites
    
    # Start monitoring
    start_monitoring
    
    # Wait for monitoring to initialize
    echo -e "${YELLOW}Waiting for monitoring stack to initialize...${NC}"
    sleep 10
    
    # Run the appropriate tests
    if [ "$TEST_TYPE" = "throughput" ] || [ "$TEST_TYPE" = "all" ]; then
        # Create output directory for throughput test
        local timestamp=$(date +"%Y%m%d_%H%M%S")
        local throughput_dir="${OUTPUT_DIR}/throughput_test_${timestamp}"
        mkdir -p "$throughput_dir"
        
        echo -e "${BLUE}Running throughput test...${NC}"
        
        # Build the load test tool
        cd "$SCRIPT_DIR/loadtest"
        go build -o loadtest loadtest.go
        
        # Get absolute path for output directory
        local abs_output_path="$(cd "$(dirname "$throughput_dir")" && pwd)/$(basename "$throughput_dir")"
        
        # Run throughput test with correct arguments
        ./loadtest \
            -url "$TARGET_URL" \
            -threads "$THREADS" \
            -duration "${DURATION}s" \
            -read-ratio "$READ_RATIO" \
            -keys "$KEY_COUNT" \
            -value-size "$VALUE_SIZE" \
            -report-interval "5s" \
            -output "$abs_output_path/throughput_results.json" \
            -rps "$RPS"
        
        echo -e "${GREEN}Throughput test completed successfully.${NC}"
        echo -e "Results saved to: ${throughput_dir}"
        
        analyze_results "$throughput_dir"
    fi
    
    if [ "$TEST_TYPE" = "stress" ] || [ "$TEST_TYPE" = "all" ]; then
        # Create output directory for stress test
        local timestamp=$(date +"%Y%m%d_%H%M%S")
        local stress_dir="${OUTPUT_DIR}/stress_test_${timestamp}"
        mkdir -p "$stress_dir"
        
        echo -e "${BLUE}Running stress test...${NC}"
        
        # Build the stress test tool
        cd "$SCRIPT_DIR/loadtest"
        go build -o stress_test stress_test.go
        
        # Get absolute path for output directory
        local abs_output_path="$(cd "$(dirname "$stress_dir")" && pwd)/$(basename "$stress_dir")"
        
        # Run stress test with correct arguments
        ./stress_test \
            -url "$TARGET_URL" \
            -threads "$THREADS" \
            -duration "${DURATION}s" \
            -keys "$KEY_COUNT" \
            -value-size "$VALUE_SIZE" \
            -report-interval "5s" \
            -output "$abs_output_path/stress_results.json" \
            -max-keys "$MAX_KEYS"
        
        echo -e "${GREEN}Stress test completed successfully.${NC}"
        echo -e "Results saved to: ${stress_dir}"
        
        analyze_results "$stress_dir"
    fi
    
    # Stop monitoring
    stop_monitoring
    
    echo -e "${GREEN}All tests completed successfully!${NC}"
}

# Run the tests
run_tests 