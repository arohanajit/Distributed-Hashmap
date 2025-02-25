#!/usr/bin/env python3

import argparse
import csv
import json
import os
import sys
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser(description='Analyze load test results')
    parser.add_argument('--results-dir', type=str, required=True, 
                        help='Directory containing test results')
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Directory to save analysis results (defaults to results-dir/analysis)')
    parser.add_argument('--compare-with', type=str, default=None,
                        help='Directory containing baseline results to compare with')
    return parser.parse_args()

def load_test_results(results_dir):
    """Load test results from the specified directory."""
    results_dir = Path(results_dir)
    
    # Load test configuration
    config_path = results_dir / 'config.json'
    if not config_path.exists():
        print(f"Error: Configuration file not found at {config_path}")
        return None, None
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Determine test type and load results
    test_type = config.get('test_type', 'unknown')
    results_file = results_dir / f"{test_type}_results.json"
    
    if not results_file.exists():
        print(f"Error: Results file not found at {results_file}")
        return config, None
    
    with open(results_file, 'r') as f:
        results = json.load(f)
    
    return config, results

def load_monitoring_data(results_dir):
    """Load monitoring data from the specified directory."""
    monitoring_dir = Path(results_dir) / 'monitoring'
    if not monitoring_dir.exists():
        print(f"Warning: Monitoring data not found at {monitoring_dir}")
        return None
    
    data = {
        'cpu': None,
        'memory': None,
        'network': None,
        'disk': None,
        'containers': {}
    }
    
    # Load CPU data
    cpu_file = monitoring_dir / 'cpu_stats.csv'
    if cpu_file.exists():
        data['cpu'] = pd.read_csv(cpu_file)
    
    # Load memory data
    memory_file = monitoring_dir / 'memory_stats.csv'
    if memory_file.exists():
        data['memory'] = pd.read_csv(memory_file)
    
    # Load network data
    network_file = monitoring_dir / 'network_stats.csv'
    if network_file.exists():
        data['network'] = pd.read_csv(network_file)
    
    # Load disk data
    disk_file = monitoring_dir / 'disk_stats.csv'
    if disk_file.exists():
        data['disk'] = pd.read_csv(disk_file)
    
    # Load container data
    for file in monitoring_dir.glob('container_*.csv'):
        container_name = file.stem.replace('container_', '')
        data['containers'][container_name] = pd.read_csv(file)
    
    return data

def analyze_throughput_test(config, results, monitoring_data, output_dir):
    """Analyze throughput test results."""
    if not results:
        print("Error: No results data available for analysis")
        return
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract key metrics
    total_requests = results.get('total_requests', 0)
    successful_requests = results.get('successful_requests', 0)
    failed_requests = results.get('failed_requests', 0)
    duration = results.get('duration_seconds', 0)
    
    if duration > 0:
        requests_per_second = total_requests / duration
    else:
        requests_per_second = 0
    
    success_rate = (successful_requests / total_requests * 100) if total_requests > 0 else 0
    
    # Get latency metrics
    latency_p50 = results.get('latency_p50_ms', 0)
    latency_p95 = results.get('latency_p95_ms', 0)
    latency_p99 = results.get('latency_p99_ms', 0)
    
    # Write summary to file
    with open(os.path.join(output_dir, 'throughput_summary.txt'), 'w') as f:
        f.write("Throughput Test Analysis\n")
        f.write("=======================\n\n")
        f.write(f"Test Duration: {duration} seconds\n")
        f.write(f"Total Requests: {total_requests}\n")
        f.write(f"Successful Requests: {successful_requests}\n")
        f.write(f"Failed Requests: {failed_requests}\n")
        f.write(f"Requests Per Second: {requests_per_second:.2f}\n")
        f.write(f"Success Rate: {success_rate:.2f}%\n\n")
        f.write(f"Latency (ms):\n")
        f.write(f"  P50: {latency_p50:.2f}\n")
        f.write(f"  P95: {latency_p95:.2f}\n")
        f.write(f"  P99: {latency_p99:.2f}\n\n")
        
        # Add status code distribution if available
        if 'status_codes' in results:
            f.write("Status Code Distribution:\n")
            for code, count in results['status_codes'].items():
                percentage = (count / total_requests * 100) if total_requests > 0 else 0
                f.write(f"  {code}: {count} ({percentage:.2f}%)\n")
    
    # Create throughput over time chart if time_series data is available
    if 'time_series' in results:
        time_series = results['time_series']
        timestamps = [entry.get('timestamp', 0) for entry in time_series]
        rps_values = [entry.get('requests_per_second', 0) for entry in time_series]
        
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, rps_values)
        plt.title('Throughput Over Time')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Requests Per Second')
        plt.grid(True)
        plt.savefig(os.path.join(output_dir, 'throughput_over_time.png'))
        plt.close()
        
        # Create latency over time chart
        latency_p50_values = [entry.get('latency_p50_ms', 0) for entry in time_series]
        latency_p95_values = [entry.get('latency_p95_ms', 0) for entry in time_series]
        latency_p99_values = [entry.get('latency_p99_ms', 0) for entry in time_series]
        
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, latency_p50_values, label='P50')
        plt.plot(timestamps, latency_p95_values, label='P95')
        plt.plot(timestamps, latency_p99_values, label='P99')
        plt.title('Latency Over Time')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Latency (ms)')
        plt.legend()
        plt.grid(True)
        plt.savefig(os.path.join(output_dir, 'latency_over_time.png'))
        plt.close()
    
    # Analyze monitoring data if available
    if monitoring_data:
        analyze_monitoring_data(monitoring_data, output_dir)

def analyze_stress_test(config, results, monitoring_data, output_dir):
    """Analyze stress test results."""
    if not results:
        print("Error: No results data available for analysis")
        return
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract key metrics
    total_requests = results.get('total_requests', 0)
    successful_requests = results.get('successful_requests', 0)
    failed_requests = results.get('failed_requests', 0)
    duration = results.get('duration_seconds', 0)
    keys_stored = results.get('keys_stored', 0)
    bytes_stored = results.get('bytes_stored', 0)
    
    if duration > 0:
        requests_per_second = total_requests / duration
        keys_per_second = keys_stored / duration
        mb_per_second = (bytes_stored / 1024 / 1024) / duration
    else:
        requests_per_second = 0
        keys_per_second = 0
        mb_per_second = 0
    
    success_rate = (successful_requests / total_requests * 100) if total_requests > 0 else 0
    
    # Write summary to file
    with open(os.path.join(output_dir, 'stress_test_summary.txt'), 'w') as f:
        f.write("Stress Test Analysis\n")
        f.write("===================\n\n")
        f.write(f"Test Duration: {duration} seconds\n")
        f.write(f"Total Requests: {total_requests}\n")
        f.write(f"Successful Requests: {successful_requests}\n")
        f.write(f"Failed Requests: {failed_requests}\n")
        f.write(f"Requests Per Second: {requests_per_second:.2f}\n")
        f.write(f"Success Rate: {success_rate:.2f}%\n\n")
        f.write(f"Keys Stored: {keys_stored}\n")
        f.write(f"Keys Per Second: {keys_per_second:.2f}\n")
        f.write(f"Data Stored: {bytes_stored / 1024 / 1024:.2f} MB\n")
        f.write(f"Data Rate: {mb_per_second:.2f} MB/s\n\n")
        
        # Add status code distribution if available
        if 'status_codes' in results:
            f.write("Status Code Distribution:\n")
            for code, count in results['status_codes'].items():
                percentage = (count / total_requests * 100) if total_requests > 0 else 0
                f.write(f"  {code}: {count} ({percentage:.2f}%)\n")
    
    # Create keys stored over time chart if time_series data is available
    if 'time_series' in results:
        time_series = results['time_series']
        timestamps = [entry.get('timestamp', 0) for entry in time_series]
        keys_values = [entry.get('keys_stored', 0) for entry in time_series]
        
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, keys_values)
        plt.title('Keys Stored Over Time')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Number of Keys')
        plt.grid(True)
        plt.savefig(os.path.join(output_dir, 'keys_over_time.png'))
        plt.close()
        
        # Create data stored over time chart
        bytes_values = [entry.get('bytes_stored', 0) / 1024 / 1024 for entry in time_series]
        
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, bytes_values)
        plt.title('Data Stored Over Time')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Data (MB)')
        plt.grid(True)
        plt.savefig(os.path.join(output_dir, 'data_over_time.png'))
        plt.close()
        
        # Create requests per second over time chart
        rps_values = [entry.get('requests_per_second', 0) for entry in time_series]
        
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, rps_values)
        plt.title('Throughput Over Time')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Requests Per Second')
        plt.grid(True)
        plt.savefig(os.path.join(output_dir, 'throughput_over_time.png'))
        plt.close()
    
    # Analyze monitoring data if available
    if monitoring_data:
        analyze_monitoring_data(monitoring_data, output_dir)

def analyze_monitoring_data(monitoring_data, output_dir):
    """Analyze system monitoring data."""
    # Create monitoring output directory
    monitoring_output_dir = os.path.join(output_dir, 'monitoring')
    os.makedirs(monitoring_output_dir, exist_ok=True)
    
    # Analyze CPU usage
    if monitoring_data['cpu'] is not None:
        cpu_df = monitoring_data['cpu']
        plt.figure(figsize=(10, 6))
        plt.plot(cpu_df['timestamp'], cpu_df['user_percent'], label='User')
        plt.plot(cpu_df['timestamp'], cpu_df['system_percent'], label='System')
        plt.plot(cpu_df['timestamp'], cpu_df['iowait_percent'], label='I/O Wait')
        plt.title('CPU Usage Over Time')
        plt.xlabel('Time')
        plt.ylabel('CPU Usage (%)')
        plt.legend()
        plt.grid(True)
        plt.savefig(os.path.join(monitoring_output_dir, 'cpu_usage.png'))
        plt.close()
    
    # Analyze memory usage
    if monitoring_data['memory'] is not None:
        memory_df = monitoring_data['memory']
        plt.figure(figsize=(10, 6))
        plt.plot(memory_df['timestamp'], memory_df['used_percent'], label='Used')
        plt.title('Memory Usage Over Time')
        plt.xlabel('Time')
        plt.ylabel('Memory Usage (%)')
        plt.legend()
        plt.grid(True)
        plt.savefig(os.path.join(monitoring_output_dir, 'memory_usage.png'))
        plt.close()
    
    # Analyze network usage
    if monitoring_data['network'] is not None:
        network_df = monitoring_data['network']
        plt.figure(figsize=(10, 6))
        
        # Find the primary network interface (with highest traffic)
        interfaces = network_df['interface'].unique()
        primary_interface = interfaces[0]  # Default to first interface
        
        if len(interfaces) > 1:
            max_traffic = 0
            for interface in interfaces:
                interface_df = network_df[network_df['interface'] == interface]
                total_traffic = interface_df['rx_bytes'].sum() + interface_df['tx_bytes'].sum()
                if total_traffic > max_traffic:
                    max_traffic = total_traffic
                    primary_interface = interface
        
        # Filter for primary interface
        interface_df = network_df[network_df['interface'] == primary_interface]
        
        # Convert to MB/s for readability
        rx_mbps = interface_df['rx_bytes'].diff() / 1024 / 1024
        tx_mbps = interface_df['tx_bytes'].diff() / 1024 / 1024
        
        plt.plot(interface_df['timestamp'], rx_mbps, label='Receive')
        plt.plot(interface_df['timestamp'], tx_mbps, label='Transmit')
        plt.title(f'Network Traffic Over Time ({primary_interface})')
        plt.xlabel('Time')
        plt.ylabel('Traffic (MB/s)')
        plt.legend()
        plt.grid(True)
        plt.savefig(os.path.join(monitoring_output_dir, 'network_traffic.png'))
        plt.close()
    
    # Analyze container metrics
    for container_name, container_df in monitoring_data['containers'].items():
        # CPU usage
        plt.figure(figsize=(10, 6))
        plt.plot(container_df['timestamp'], container_df['cpu_percent'])
        plt.title(f'Container CPU Usage: {container_name}')
        plt.xlabel('Time')
        plt.ylabel('CPU Usage (%)')
        plt.grid(True)
        plt.savefig(os.path.join(monitoring_output_dir, f'{container_name}_cpu.png'))
        plt.close()
        
        # Memory usage
        plt.figure(figsize=(10, 6))
        # Convert to MB for readability
        memory_mb = container_df['memory_usage'] / 1024 / 1024
        plt.plot(container_df['timestamp'], memory_mb)
        plt.title(f'Container Memory Usage: {container_name}')
        plt.xlabel('Time')
        plt.ylabel('Memory Usage (MB)')
        plt.grid(True)
        plt.savefig(os.path.join(monitoring_output_dir, f'{container_name}_memory.png'))
        plt.close()

def compare_results(current_results, baseline_results, output_dir):
    """Compare current test results with baseline results."""
    if not current_results or not baseline_results:
        print("Error: Missing data for comparison")
        return
    
    # Create comparison output directory
    comparison_dir = os.path.join(output_dir, 'comparison')
    os.makedirs(comparison_dir, exist_ok=True)
    
    # Extract key metrics from both results
    current_config, current_data = current_results
    baseline_config, baseline_data = baseline_results
    
    # Determine test type
    test_type = current_config.get('test_type', 'unknown')
    
    # Common metrics for both test types
    metrics = {
        'total_requests': ('Total Requests', ''),
        'successful_requests': ('Successful Requests', ''),
        'failed_requests': ('Failed Requests', ''),
        'duration_seconds': ('Duration', 'seconds'),
    }
    
    # Add test-specific metrics
    if test_type == 'throughput':
        metrics.update({
            'latency_p50_ms': ('P50 Latency', 'ms'),
            'latency_p95_ms': ('P95 Latency', 'ms'),
            'latency_p99_ms': ('P99 Latency', 'ms'),
        })
    elif test_type == 'stress':
        metrics.update({
            'keys_stored': ('Keys Stored', ''),
            'bytes_stored': ('Data Stored', 'bytes'),
        })
    
    # Calculate derived metrics
    if current_data.get('duration_seconds', 0) > 0:
        current_rps = current_data.get('total_requests', 0) / current_data.get('duration_seconds', 1)
    else:
        current_rps = 0
    
    if baseline_data.get('duration_seconds', 0) > 0:
        baseline_rps = baseline_data.get('total_requests', 0) / baseline_data.get('duration_seconds', 1)
    else:
        baseline_rps = 0
    
    # Add derived metrics
    metrics['rps'] = ('Requests Per Second', '')
    current_data['rps'] = current_rps
    baseline_data['rps'] = baseline_rps
    
    # Write comparison to file
    with open(os.path.join(comparison_dir, 'comparison_summary.txt'), 'w') as f:
        f.write(f"{test_type.capitalize()} Test Comparison\n")
        f.write("=" * (len(test_type) + 15) + "\n\n")
        f.write("Current Test: " + current_config.get('timestamp', 'Unknown') + "\n")
        f.write("Baseline Test: " + baseline_config.get('timestamp', 'Unknown') + "\n\n")
        
        f.write("Metric Comparison:\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Metric':<25} {'Baseline':<15} {'Current':<15} {'Change':<15} {'Change %':<10}\n")
        f.write("-" * 80 + "\n")
        
        for key, (label, unit) in metrics.items():
            baseline_value = baseline_data.get(key, 0)
            current_value = current_data.get(key, 0)
            
            if baseline_value != 0:
                change = current_value - baseline_value
                change_percent = (change / baseline_value) * 100
                change_str = f"{change:+.2f} {unit}"
                change_percent_str = f"{change_percent:+.2f}%"
            else:
                change_str = "N/A"
                change_percent_str = "N/A"
            
            f.write(f"{label:<25} {baseline_value:<15.2f} {current_value:<15.2f} {change_str:<15} {change_percent_str:<10}\n")
    
    # Create comparison charts
    # Requests per second comparison
    labels = ['Baseline', 'Current']
    values = [baseline_rps, current_rps]
    
    plt.figure(figsize=(8, 6))
    plt.bar(labels, values)
    plt.title('Requests Per Second Comparison')
    plt.ylabel('Requests Per Second')
    plt.grid(axis='y')
    
    # Add value labels on top of bars
    for i, v in enumerate(values):
        plt.text(i, v + 0.1, f"{v:.2f}", ha='center')
    
    plt.savefig(os.path.join(comparison_dir, 'rps_comparison.png'))
    plt.close()
    
    # Test-specific comparisons
    if test_type == 'throughput':
        # Latency comparison
        metrics = ['latency_p50_ms', 'latency_p95_ms', 'latency_p99_ms']
        labels = ['P50', 'P95', 'P99']
        baseline_values = [baseline_data.get(m, 0) for m in metrics]
        current_values = [current_data.get(m, 0) for m in metrics]
        
        x = np.arange(len(labels))
        width = 0.35
        
        plt.figure(figsize=(10, 6))
        plt.bar(x - width/2, baseline_values, width, label='Baseline')
        plt.bar(x + width/2, current_values, width, label='Current')
        plt.title('Latency Comparison')
        plt.ylabel('Latency (ms)')
        plt.xlabel('Percentile')
        plt.xticks(x, labels)
        plt.legend()
        plt.grid(axis='y')
        
        plt.savefig(os.path.join(comparison_dir, 'latency_comparison.png'))
        plt.close()
    
    elif test_type == 'stress':
        # Keys stored comparison
        labels = ['Baseline', 'Current']
        baseline_keys = baseline_data.get('keys_stored', 0)
        current_keys = current_data.get('keys_stored', 0)
        values = [baseline_keys, current_keys]
        
        plt.figure(figsize=(8, 6))
        plt.bar(labels, values)
        plt.title('Keys Stored Comparison')
        plt.ylabel('Number of Keys')
        plt.grid(axis='y')
        
        # Add value labels on top of bars
        for i, v in enumerate(values):
            plt.text(i, v + 0.1, f"{v:.0f}", ha='center')
        
        plt.savefig(os.path.join(comparison_dir, 'keys_comparison.png'))
        plt.close()
        
        # Data stored comparison
        baseline_bytes = baseline_data.get('bytes_stored', 0) / 1024 / 1024  # Convert to MB
        current_bytes = current_data.get('bytes_stored', 0) / 1024 / 1024  # Convert to MB
        values = [baseline_bytes, current_bytes]
        
        plt.figure(figsize=(8, 6))
        plt.bar(labels, values)
        plt.title('Data Stored Comparison')
        plt.ylabel('Data (MB)')
        plt.grid(axis='y')
        
        # Add value labels on top of bars
        for i, v in enumerate(values):
            plt.text(i, v + 0.1, f"{v:.2f}", ha='center')
        
        plt.savefig(os.path.join(comparison_dir, 'data_comparison.png'))
        plt.close()

def main():
    args = parse_args()
    
    # Set output directory
    if args.output_dir is None:
        args.output_dir = os.path.join(args.results_dir, 'analysis')
    
    # Load test results
    config, results = load_test_results(args.results_dir)
    if config is None:
        print("Error: Failed to load test configuration")
        return 1
    
    # Load monitoring data
    monitoring_data = load_monitoring_data(args.results_dir)
    
    # Determine test type and analyze accordingly
    test_type = config.get('test_type', 'unknown')
    if test_type == 'throughput':
        analyze_throughput_test(config, results, monitoring_data, args.output_dir)
    elif test_type == 'stress':
        analyze_stress_test(config, results, monitoring_data, args.output_dir)
    else:
        print(f"Error: Unknown test type: {test_type}")
        return 1
    
    # Compare with baseline if specified
    if args.compare_with:
        baseline_config, baseline_results = load_test_results(args.compare_with)
        if baseline_config is not None and baseline_results is not None:
            compare_results((config, results), (baseline_config, baseline_results), args.output_dir)
        else:
            print("Warning: Failed to load baseline results for comparison")
    
    print(f"Analysis complete. Results saved to {args.output_dir}")
    return 0

if __name__ == '__main__':
    sys.exit(main()) 