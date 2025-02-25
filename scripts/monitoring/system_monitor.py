#!/usr/bin/env python3

import argparse
import csv
import datetime
import json
import os
import subprocess
import sys
import time
from typing import Dict, List, Optional, Tuple, Union

# Default settings
DEFAULT_INTERVAL = 5  # seconds
DEFAULT_OUTPUT_DIR = "monitoring_results"
DEFAULT_CONTAINERS = []
DEFAULT_DURATION = 300  # seconds


class SystemMonitor:
    def __init__(
        self,
        interval: int = DEFAULT_INTERVAL,
        output_dir: str = DEFAULT_OUTPUT_DIR,
        containers: List[str] = None,
        duration: int = DEFAULT_DURATION,
    ):
        self.interval = interval
        self.output_dir = output_dir
        
        # Handle special case where 'none' is passed as a container name
        if containers and len(containers) == 1 and containers[0].lower() == 'none':
            self.containers = []
            print("No containers specified for monitoring, only host metrics will be collected.")
        else:
            self.containers = containers or DEFAULT_CONTAINERS
            
        self.duration = duration
        self.start_time = None
        self.end_time = None
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize data structures
        self.cpu_data = []
        self.memory_data = []
        self.network_data = []
        self.disk_data = []
        self.container_data = []
        
        # CSV file paths
        self.cpu_file = os.path.join(output_dir, "cpu_usage.csv")
        self.memory_file = os.path.join(output_dir, "memory_usage.csv")
        self.network_file = os.path.join(output_dir, "network_usage.csv")
        self.disk_file = os.path.join(output_dir, "disk_usage.csv")
        self.container_file = os.path.join(output_dir, "container_usage.csv")
        
        # Initialize CSV files
        self._init_csv_files()
    
    def _init_csv_files(self):
        """Initialize CSV files with headers."""
        with open(self.cpu_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "cpu_percent", "user", "system", "idle", "iowait"])
        
        with open(self.memory_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "total_mb", "used_mb", "free_mb", "cached_mb", "buffers_mb", "percent_used"])
        
        with open(self.network_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "interface", "rx_bytes", "rx_packets", "tx_bytes", "tx_packets"])
        
        with open(self.disk_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "device", "reads", "writes", "read_bytes", "write_bytes", "busy_percent"])
        
        if self.containers:
            with open(self.container_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "container", "cpu_percent", "memory_usage_mb", "memory_limit_mb", "memory_percent", "network_rx_bytes", "network_tx_bytes"])
    
    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.datetime.now().isoformat()
    
    def _run_command(self, command: List[str]) -> str:
        """Run a shell command and return its output."""
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            print(f"Error running command {' '.join(command)}: {e}", file=sys.stderr)
            return ""
        except FileNotFoundError as e:
            print(f"Command not found: {command[0]}", file=sys.stderr)
            return ""
    
    def _command_exists(self, command: str) -> bool:
        """Check if a command exists in the system."""
        try:
            subprocess.run(["which", command], capture_output=True, check=True)
            return True
        except subprocess.CalledProcessError:
            return False
        except FileNotFoundError:
            return False
    
    def collect_cpu_stats(self):
        """Collect CPU statistics."""
        timestamp = self._get_timestamp()
        
        cpu_percent = 0.0
        user = 0.0
        system = 0.0
        idle = 0.0
        iowait = 0.0
        
        # Try different methods to get CPU usage based on available commands
        if self._command_exists("mpstat"):
            # Get CPU usage with mpstat (Linux)
            mpstat_output = self._run_command(["mpstat", "1", "1"])
            if mpstat_output:
                # Parse mpstat output
                for line in mpstat_output.splitlines():
                    if "all" in line:
                        parts = line.split()
                        if len(parts) >= 12:
                            user = float(parts[3])
                            system = float(parts[5])
                            idle = float(parts[11])
                            iowait = float(parts[6]) if len(parts) > 6 else 0.0
                            cpu_percent = 100.0 - idle
        elif self._command_exists("top"):
            # Fallback to top (macOS and Linux)
            if sys.platform == "darwin":
                # macOS specific top command
                top_output = self._run_command(["top", "-l", "1", "-n", "0", "-s", "0"])
                if top_output:
                    cpu_line = next((line for line in top_output.splitlines() if "CPU usage" in line), "")
                    if cpu_line:
                        try:
                            # Parse macOS top output which looks like:
                            # "CPU usage: 47.36% user, 11.76% sys, 40.86% idle"
                            parts = cpu_line.split(":")
                            if len(parts) > 1:
                                usage_parts = parts[1].split(",")
                                if len(usage_parts) >= 3:
                                    # Extract percentages from strings like "47.36% user"
                                    user = float(usage_parts[0].strip().split("%")[0].strip())
                                    system = float(usage_parts[1].strip().split("%")[0].strip())
                                    idle = float(usage_parts[2].strip().split("%")[0].strip())
                                    cpu_percent = user + system
                        except (ValueError, IndexError) as e:
                            print(f"Error parsing macOS top output: {e}", file=sys.stderr)
                            print(f"CPU line: {cpu_line}", file=sys.stderr)
                            # Use psutil as fallback
                            try:
                                import psutil
                                cpu_percent = psutil.cpu_percent(interval=1)
                                user = cpu_percent * 0.7  # Estimate user as 70% of total
                                system = cpu_percent * 0.3  # Estimate system as 30% of total
                                idle = 100.0 - cpu_percent
                            except ImportError:
                                pass
            else:
                # Linux top command
                top_output = self._run_command(["top", "-bn1"])
                if top_output:
                    cpu_line = next((line for line in top_output.splitlines() if "%Cpu" in line), "")
                    if cpu_line:
                        parts = cpu_line.split(":")
                        if len(parts) > 1:
                            usage_parts = parts[1].split(",")
                            for part in usage_parts:
                                if "us" in part:
                                    user = float(part.strip().split()[0])
                                elif "sy" in part:
                                    system = float(part.strip().split()[0])
                                elif "id" in part:
                                    idle = float(part.strip().split()[0])
                                elif "wa" in part:
                                    iowait = float(part.strip().split()[0])
                            cpu_percent = 100.0 - idle
        else:
            # Last resort: use psutil if available
            try:
                import psutil
                cpu_percent = psutil.cpu_percent(interval=1)
                cpu_times = psutil.cpu_times_percent(interval=0)
                user = cpu_times.user
                system = cpu_times.system
                idle = cpu_times.idle
                iowait = getattr(cpu_times, 'iowait', 0.0)
            except (ImportError, AttributeError) as e:
                print(f"Error using psutil: {e}", file=sys.stderr)
                # Set some default values
                cpu_percent = 0.0
                user = 0.0
                system = 0.0
                idle = 100.0
                iowait = 0.0
        
        # Write to CSV
        with open(self.cpu_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, cpu_percent, user, system, idle, iowait])
        
        # Store in memory
        self.cpu_data.append({
            "timestamp": timestamp,
            "cpu_percent": cpu_percent,
            "user": user,
            "system": system,
            "idle": idle,
            "iowait": iowait
        })
    
    def collect_memory_stats(self):
        """Collect memory statistics."""
        timestamp = self._get_timestamp()
        
        # Get memory usage with free
        free_output = self._run_command(["free", "-m"])
        if not free_output:
            total_mb, used_mb, free_mb, cached_mb, buffers_mb, percent_used = 0, 0, 0, 0, 0, 0
        else:
            lines = free_output.splitlines()
            if len(lines) >= 2:
                mem_line = lines[1]
                parts = mem_line.split()
                if len(parts) >= 7:
                    total_mb = int(parts[1])
                    used_mb = int(parts[2])
                    free_mb = int(parts[3])
                    buffers_mb = int(parts[5])
                    cached_mb = int(parts[6])
                    percent_used = (used_mb / total_mb) * 100 if total_mb > 0 else 0
                else:
                    total_mb, used_mb, free_mb, cached_mb, buffers_mb, percent_used = 0, 0, 0, 0, 0, 0
            else:
                total_mb, used_mb, free_mb, cached_mb, buffers_mb, percent_used = 0, 0, 0, 0, 0, 0
        
        # Save data
        self.memory_data.append({
            "timestamp": timestamp,
            "total_mb": total_mb,
            "used_mb": used_mb,
            "free_mb": free_mb,
            "cached_mb": cached_mb,
            "buffers_mb": buffers_mb,
            "percent_used": percent_used
        })
        
        # Write to CSV
        with open(self.memory_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, total_mb, used_mb, free_mb, cached_mb, buffers_mb, percent_used])
    
    def collect_network_stats(self):
        """Collect network statistics."""
        timestamp = self._get_timestamp()
        
        # Get network usage from /proc/net/dev
        try:
            with open("/proc/net/dev", "r") as f:
                lines = f.readlines()
                
                interfaces = []
                for line in lines[2:]:  # Skip header lines
                    parts = line.split(":")
                    if len(parts) == 2:
                        interface = parts[0].strip()
                        values = parts[1].split()
                        if len(values) >= 16:
                            rx_bytes = int(values[0])
                            rx_packets = int(values[1])
                            tx_bytes = int(values[8])
                            tx_packets = int(values[9])
                            
                            interfaces.append({
                                "interface": interface,
                                "rx_bytes": rx_bytes,
                                "rx_packets": rx_packets,
                                "tx_bytes": tx_bytes,
                                "tx_packets": tx_packets
                            })
                
                # Save data
                for interface_data in interfaces:
                    self.network_data.append({
                        "timestamp": timestamp,
                        **interface_data
                    })
                    
                    # Write to CSV
                    with open(self.network_file, "a", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            timestamp,
                            interface_data["interface"],
                            interface_data["rx_bytes"],
                            interface_data["rx_packets"],
                            interface_data["tx_bytes"],
                            interface_data["tx_packets"]
                        ])
        except Exception as e:
            print(f"Error collecting network stats: {e}", file=sys.stderr)
    
    def collect_disk_stats(self):
        """Collect disk statistics."""
        timestamp = self._get_timestamp()
        
        # Get disk usage with iostat
        iostat_output = self._run_command(["iostat", "-dx", "1", "1"])
        if not iostat_output:
            return
        
        lines = iostat_output.splitlines()
        devices = []
        
        # Find the header line
        header_index = -1
        for i, line in enumerate(lines):
            if "Device" in line and "r/s" in line:
                header_index = i
                break
        
        if header_index >= 0 and header_index + 1 < len(lines):
            for line in lines[header_index + 1:]:
                parts = line.split()
                if len(parts) >= 14:
                    device = parts[0]
                    reads = float(parts[3])
                    writes = float(parts[4])
                    read_bytes = float(parts[5]) * 1024  # Convert to bytes
                    write_bytes = float(parts[6]) * 1024  # Convert to bytes
                    busy_percent = float(parts[13])
                    
                    devices.append({
                        "device": device,
                        "reads": reads,
                        "writes": writes,
                        "read_bytes": read_bytes,
                        "write_bytes": write_bytes,
                        "busy_percent": busy_percent
                    })
        
        # Save data
        for device_data in devices:
            self.disk_data.append({
                "timestamp": timestamp,
                **device_data
            })
            
            # Write to CSV
            with open(self.disk_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp,
                    device_data["device"],
                    device_data["reads"],
                    device_data["writes"],
                    device_data["read_bytes"],
                    device_data["write_bytes"],
                    device_data["busy_percent"]
                ])
    
    def collect_container_stats(self):
        """Collect Docker container statistics."""
        if not self.containers:
            return
        
        timestamp = self._get_timestamp()
        
        for container in self.containers:
            # Get container stats
            stats_output = self._run_command(["docker", "stats", container, "--no-stream", "--format", "{{json .}}"])
            if not stats_output:
                continue
            
            try:
                stats = json.loads(stats_output)
                
                # Extract CPU usage
                cpu_percent = float(stats.get("CPUPerc", "0%").rstrip("%"))
                
                # Extract memory usage
                memory_usage_str = stats.get("MemUsage", "0B / 0B")
                memory_parts = memory_usage_str.split(" / ")
                memory_usage = memory_parts[0]
                memory_limit = memory_parts[1] if len(memory_parts) > 1 else "0B"
                
                # Convert memory values to MB
                memory_usage_mb = self._convert_to_mb(memory_usage)
                memory_limit_mb = self._convert_to_mb(memory_limit)
                memory_percent = (memory_usage_mb / memory_limit_mb) * 100 if memory_limit_mb > 0 else 0
                
                # Extract network usage
                network_io = stats.get("NetIO", "0B / 0B")
                network_parts = network_io.split(" / ")
                network_rx = network_parts[0]
                network_tx = network_parts[1] if len(network_parts) > 1 else "0B"
                
                # Convert network values to bytes
                network_rx_bytes = self._convert_to_bytes(network_rx)
                network_tx_bytes = self._convert_to_bytes(network_tx)
                
                # Save data
                self.container_data.append({
                    "timestamp": timestamp,
                    "container": container,
                    "cpu_percent": cpu_percent,
                    "memory_usage_mb": memory_usage_mb,
                    "memory_limit_mb": memory_limit_mb,
                    "memory_percent": memory_percent,
                    "network_rx_bytes": network_rx_bytes,
                    "network_tx_bytes": network_tx_bytes
                })
                
                # Write to CSV
                with open(self.container_file, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        timestamp,
                        container,
                        cpu_percent,
                        memory_usage_mb,
                        memory_limit_mb,
                        memory_percent,
                        network_rx_bytes,
                        network_tx_bytes
                    ])
            except json.JSONDecodeError:
                print(f"Error parsing JSON for container {container}", file=sys.stderr)
            except Exception as e:
                print(f"Error collecting stats for container {container}: {e}", file=sys.stderr)
    
    def _convert_to_mb(self, value_str: str) -> float:
        """Convert a memory string (e.g., '1.5GiB') to MB."""
        try:
            value = float(value_str.rstrip("BKMGTPEZYkgmtpezy"))
            unit = value_str.lstrip("0123456789.").upper()
            
            if "K" in unit:
                return value / 1024
            elif "M" in unit:
                return value
            elif "G" in unit:
                return value * 1024
            elif "T" in unit:
                return value * 1024 * 1024
            else:
                return value / (1024 * 1024)  # Assume bytes
        except ValueError:
            return 0.0
    
    def _convert_to_bytes(self, value_str: str) -> float:
        """Convert a data size string (e.g., '1.5GB') to bytes."""
        try:
            value = float(value_str.rstrip("BKMGTPEZYkgmtpezy"))
            unit = value_str.lstrip("0123456789.").upper()
            
            if "K" in unit:
                return value * 1024
            elif "M" in unit:
                return value * 1024 * 1024
            elif "G" in unit:
                return value * 1024 * 1024 * 1024
            elif "T" in unit:
                return value * 1024 * 1024 * 1024 * 1024
            else:
                return value  # Assume bytes
        except ValueError:
            return 0.0
    
    def collect_all_stats(self):
        """Collect all system statistics."""
        self.collect_cpu_stats()
        self.collect_memory_stats()
        self.collect_network_stats()
        self.collect_disk_stats()
        self.collect_container_stats()
    
    def start_monitoring(self):
        """Start monitoring system resources."""
        print(f"Starting system monitoring (interval: {self.interval}s, duration: {self.duration}s)")
        print(f"Results will be saved to: {self.output_dir}")
        
        self.start_time = datetime.datetime.now()
        end_time = self.start_time + datetime.timedelta(seconds=self.duration)
        
        try:
            while datetime.datetime.now() < end_time:
                self.collect_all_stats()
                
                # Print current stats
                if self.memory_data:
                    memory = self.memory_data[-1]
                    print(f"Memory: {memory['used_mb']}/{memory['total_mb']} MB ({memory['percent_used']:.1f}%)", end=" | ")
                
                if self.cpu_data:
                    cpu = self.cpu_data[-1]
                    print(f"CPU: {cpu['cpu_percent']:.1f}%", end=" | ")
                
                if self.container_data:
                    container = self.container_data[-1]
                    print(f"Container {container['container']}: CPU {container['cpu_percent']:.1f}%, Mem {container['memory_percent']:.1f}%", end="")
                
                print()  # New line
                
                # Sleep until next interval
                time.sleep(self.interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
        finally:
            self.end_time = datetime.datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Monitoring completed. Duration: {duration:.1f} seconds")
            print(f"Results saved to: {self.output_dir}")
    
    def generate_summary(self):
        """Generate a summary of the monitoring results."""
        summary_file = os.path.join(self.output_dir, "summary.txt")
        
        with open(summary_file, "w") as f:
            f.write("=== System Monitoring Summary ===\n\n")
            
            if self.start_time and self.end_time:
                duration = (self.end_time - self.start_time).total_seconds()
                f.write(f"Start Time: {self.start_time.isoformat()}\n")
                f.write(f"End Time: {self.end_time.isoformat()}\n")
                f.write(f"Duration: {duration:.1f} seconds\n\n")
            
            if self.cpu_data:
                cpu_percents = [data["cpu_percent"] for data in self.cpu_data]
                f.write("CPU Usage:\n")
                f.write(f"  Min: {min(cpu_percents):.1f}%\n")
                f.write(f"  Max: {max(cpu_percents):.1f}%\n")
                f.write(f"  Avg: {sum(cpu_percents) / len(cpu_percents):.1f}%\n\n")
            
            if self.memory_data:
                memory_percents = [data["percent_used"] for data in self.memory_data]
                memory_used = [data["used_mb"] for data in self.memory_data]
                f.write("Memory Usage:\n")
                f.write(f"  Min: {min(memory_percents):.1f}%\n")
                f.write(f"  Max: {max(memory_percents):.1f}%\n")
                f.write(f"  Avg: {sum(memory_percents) / len(memory_percents):.1f}%\n")
                f.write(f"  Peak: {max(memory_used):.1f} MB\n\n")
            
            if self.container_data:
                containers = set(data["container"] for data in self.container_data)
                for container in containers:
                    container_data = [data for data in self.container_data if data["container"] == container]
                    cpu_percents = [data["cpu_percent"] for data in container_data]
                    memory_percents = [data["memory_percent"] for data in container_data]
                    
                    f.write(f"Container {container}:\n")
                    f.write(f"  CPU Usage:\n")
                    f.write(f"    Min: {min(cpu_percents):.1f}%\n")
                    f.write(f"    Max: {max(cpu_percents):.1f}%\n")
                    f.write(f"    Avg: {sum(cpu_percents) / len(cpu_percents):.1f}%\n")
                    f.write(f"  Memory Usage:\n")
                    f.write(f"    Min: {min(memory_percents):.1f}%\n")
                    f.write(f"    Max: {max(memory_percents):.1f}%\n")
                    f.write(f"    Avg: {sum(memory_percents) / len(memory_percents):.1f}%\n\n")
        
        print(f"Summary saved to: {summary_file}")


def main():
    parser = argparse.ArgumentParser(description="Monitor system resources during load tests")
    parser.add_argument("-i", "--interval", type=int, default=DEFAULT_INTERVAL, help="Monitoring interval in seconds")
    parser.add_argument("-o", "--output-dir", type=str, default=DEFAULT_OUTPUT_DIR, help="Output directory for results")
    parser.add_argument("-c", "--containers", type=str, default="", help="Comma-separated list of Docker containers to monitor")
    parser.add_argument("-d", "--duration", type=int, default=DEFAULT_DURATION, help="Monitoring duration in seconds")
    
    args = parser.parse_args()
    
    # Parse container list from comma-separated string
    container_list = []
    if args.containers:
        container_list = [c.strip() for c in args.containers.split(",") if c.strip()]
    
    monitor = SystemMonitor(
        interval=args.interval,
        output_dir=args.output_dir,
        containers=container_list,
        duration=args.duration
    )
    
    monitor.start_monitoring()
    monitor.generate_summary()


if __name__ == "__main__":
    main() 