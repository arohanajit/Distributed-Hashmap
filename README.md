# Distributed Hashmap Load Testing Framework

A comprehensive framework for load testing and monitoring a distributed hashmap system.

## Overview

This framework provides tools for:

1. **Throughput Testing** - Measure the system's ability to handle a consistent load
2. **Stress Testing** - Push the system beyond its limits to identify breaking points
3. **System Monitoring** - Track resource usage during tests
4. **Results Analysis** - Generate reports and visualizations from test data

## Directory Structure

```
scripts/
├── loadtest/                  # Load testing tools
│   ├── loadtest.go            # Throughput testing tool
│   ├── stress_test.go         # Stress testing tool
│   ├── run_load_test.sh       # Script to run load tests
│   └── analyze_results.py     # Script to analyze test results
├── monitoring/                # Monitoring tools
│   ├── system_monitor.py      # System resource monitoring
│   ├── prometheus.yml         # Prometheus configuration
│   ├── docker-compose.monitoring.yml  # Monitoring stack
│   └── start_monitoring.sh    # Script to start monitoring
└── requirements.txt           # Python dependencies
```

## Prerequisites

- Go 1.16 or later
- Python 3.6 or later
- Docker and Docker Compose
- The distributed hashmap system running and accessible

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/distributed-hashmap.git
   cd distributed-hashmap
   ```

2. Install Python dependencies:
   ```bash
   pip install -r scripts/requirements.txt
   ```

3. Make scripts executable:
   ```bash
   chmod +x scripts/loadtest/run_load_test.sh
   chmod +x scripts/monitoring/start_monitoring.sh
   chmod +x scripts/loadtest/analyze_results.py
   ```

## Usage

### Start the Monitoring Stack

```bash
cd scripts/monitoring
./start_monitoring.sh
```

This will start:
- Prometheus (http://localhost:9090)
- Grafana (http://localhost:3000)
- Node Exporter (http://localhost:9100/metrics)
- cAdvisor (http://localhost:8080)

### Run a Throughput Test

```bash
cd scripts/loadtest
./run_load_test.sh --test-type throughput --url http://localhost:8080 --duration 300 --threads 20 --rps 5000
```

### Run a Stress Test

```bash
cd scripts/loadtest
./run_load_test.sh --test-type stress --url http://localhost:8080 --duration 600 --threads 50 --max-keys 500000
```

### Analyze Test Results

```bash
cd scripts/loadtest
./analyze_results.py --results-dir ./results/throughput_test_20230501_120000
```

To compare with a baseline:

```bash
./analyze_results.py --results-dir ./results/throughput_test_20230501_120000 --compare-with ./results/throughput_test_20230430_120000
```

## Key Features

### Throughput Testing

- Configurable request rate (RPS)
- Adjustable read/write ratio
- Customizable key and value sizes
- Real-time metrics reporting

### Stress Testing

- Progressive key insertion
- Memory usage monitoring
- Goroutine leak detection
- System resource tracking

### Monitoring

- CPU, memory, disk, and network usage
- Container-level metrics
- Custom Grafana dashboards
- Prometheus metric collection

### Analysis

- Performance metrics calculation
- Visualization of results
- Comparison with baseline tests
- Detailed reports generation

## Best Practices

1. **Establish Baselines** - Run tests against a known good configuration
2. **Isolate Variables** - Change one parameter at a time
3. **Realistic Data** - Use realistic key and value sizes
4. **Warm-up Period** - Allow the system to warm up before measuring
5. **Repeat Tests** - Run tests multiple times for consistency
6. **Monitor Everything** - Collect as much data as possible
7. **Compare Results** - Compare before and after optimizations

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.