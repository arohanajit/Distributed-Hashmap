# Distributed Hashmap Load Testing Framework

This directory contains scripts and tools for load testing and monitoring the distributed hashmap system.

## Directory Structure

- `loadtest/` - Load testing tools and scripts
  - `loadtest.go` - Go-based throughput testing tool
  - `stress_test.go` - Go-based stress testing tool
  - `run_load_test.sh` - Script to run load tests and collect metrics
- `monitoring/` - Monitoring tools and configurations
  - `system_monitor.py` - Python script for monitoring system resources
  - `prometheus.yml` - Prometheus configuration for metric collection
  - `docker-compose.monitoring.yml` - Docker Compose file for the monitoring stack
  - `start_monitoring.sh` - Script to start the monitoring stack

## Prerequisites

- Go 1.16 or later
- Python 3.6 or later
- Docker and Docker Compose
- The distributed hashmap system running and accessible

## Quick Start

### Start the Monitoring Stack

```bash
cd scripts/monitoring
chmod +x start_monitoring.sh
./start_monitoring.sh
```

This will start Prometheus, Grafana, Node Exporter, and cAdvisor for monitoring the system. You can access:
- Grafana at http://localhost:3000 (default credentials: admin/admin)
- Prometheus at http://localhost:9090
- Node Exporter metrics at http://localhost:9100/metrics
- cAdvisor at http://localhost:8080

### Run a Load Test

```bash
cd scripts/loadtest
chmod +x run_load_test.sh
./run_load_test.sh --url http://localhost:8080 --duration 60 --threads 10 --test-type throughput
```

## Load Testing Options

The `run_load_test.sh` script supports the following options:

```
Usage: ./run_load_test.sh [options]
Options:
  --url URL                  Target URL (default: http://localhost:8080)
  --duration SECONDS         Test duration in seconds (default: 60)
  --threads COUNT            Number of threads (default: 10)
  --read-ratio RATIO         Read ratio (0.0-1.0, default: 0.8)
  --key-count COUNT          Number of keys (default: 1000)
  --value-size BYTES         Value size in bytes (default: 1024)
  --output-dir DIR           Output directory (default: ./results)
  --monitor-containers LIST  Comma-separated list of containers to monitor
  --test-type TYPE           Test type: throughput or stress (default: throughput)
  --rps COUNT                Requests per second (for throughput test, default: 1000)
  --max-keys COUNT           Maximum keys (for stress test, default: 100000)
  --help                     Show this help message
```

### Throughput Testing

Throughput testing focuses on measuring the system's ability to handle a consistent load:

```bash
./run_load_test.sh --test-type throughput --rps 5000 --duration 300 --threads 20
```

This will:
- Generate a consistent load of 5000 requests per second
- Run for 5 minutes (300 seconds)
- Use 20 concurrent threads
- Collect system metrics during the test

### Stress Testing

Stress testing focuses on pushing the system beyond its limits:

```bash
./run_load_test.sh --test-type stress --max-keys 500000 --duration 600 --threads 50
```

This will:
- Push the system to store up to 500,000 keys
- Run for 10 minutes (600 seconds)
- Use 50 concurrent threads
- Collect system metrics during the test

## Monitoring

The monitoring stack includes:

1. **Prometheus** - For collecting and storing metrics
2. **Grafana** - For visualizing metrics with pre-configured dashboards
3. **Node Exporter** - For collecting host-level metrics (CPU, memory, disk, network)
4. **cAdvisor** - For collecting container-level metrics

### System Monitor

The `system_monitor.py` script collects detailed system metrics and writes them to CSV files:

```bash
python3 monitoring/system_monitor.py --interval 1 --output-dir ./monitoring_data --containers dhashmap-node1,dhashmap-node2 --duration 300
```

Options:
- `--interval` - Sampling interval in seconds
- `--output-dir` - Directory to store metrics
- `--containers` - Comma-separated list of Docker containers to monitor
- `--duration` - Duration in seconds to collect metrics

## Analyzing Results

After running a load test, results are saved to the specified output directory:
- Test configuration in `config.json`
- Test results in `throughput_results.json` or `stress_results.json`
- System monitoring data in the `monitoring/` subdirectory
- A summary report in `summary.md`

## Best Practices

1. **Establish Baselines** - Run tests against a known good configuration to establish baseline metrics
2. **Isolate Variables** - Change one parameter at a time to understand its impact
3. **Realistic Data** - Use realistic key and value sizes based on your expected workload
4. **Warm-up Period** - Allow the system to warm up before measuring performance
5. **Repeat Tests** - Run tests multiple times to ensure consistent results
6. **Monitor Everything** - Collect as much data as possible during tests
7. **Compare Results** - Compare results before and after optimizations

## Troubleshooting

- If the monitoring stack fails to start, check Docker logs with `docker-compose -f docker-compose.monitoring.yml logs`
- If load tests fail, check for network connectivity issues or resource constraints
- For permission issues with scripts, ensure they are executable with `chmod +x script_name.sh` 