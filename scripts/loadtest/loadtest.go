package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Configuration options
var (
	targetURL      string
	numThreads     int
	duration       time.Duration
	rampUpTime     time.Duration
	readWriteRatio float64
	keyCount       int
	valueSize      int
	reportInterval time.Duration
	keyPrefix      string
	valuePrefix    string
	outputFile     string
	requestsPerSec int
	maxConcurrency int
	warmupDuration time.Duration
	warmupKeyCount int
)

// Statistics
type Stats struct {
	TotalRequests   int64
	SuccessRequests int64
	FailedRequests  int64
	ReadRequests    int64
	WriteRequests   int64
	TotalLatency    int64 // in microseconds
	MinLatency      int64 // in microseconds
	MaxLatency      int64 // in microseconds
	Latencies       []int64
	StatusCodes     map[int]int64
	StartTime       time.Time
	EndTime         time.Time
	RequestsPerSec  float64
	AverageLatency  float64 // in milliseconds
	P50Latency      float64 // in milliseconds
	P90Latency      float64 // in milliseconds
	P95Latency      float64 // in milliseconds
	P99Latency      float64 // in milliseconds
	ErrorRate       float64
	mu              sync.Mutex
}

func NewStats() *Stats {
	return &Stats{
		MinLatency:  math.MaxInt64,
		Latencies:   make([]int64, 0, 1000000),
		StatusCodes: make(map[int]int64),
		StartTime:   time.Now(),
	}
}

func (s *Stats) AddLatency(latency time.Duration) {
	latencyMicros := latency.Microseconds()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.TotalLatency += latencyMicros
	s.Latencies = append(s.Latencies, latencyMicros)

	if latencyMicros < s.MinLatency {
		s.MinLatency = latencyMicros
	}

	if latencyMicros > s.MaxLatency {
		s.MaxLatency = latencyMicros
	}
}

func (s *Stats) AddStatusCode(statusCode int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.StatusCodes[statusCode]++

	if statusCode >= 200 && statusCode < 300 {
		atomic.AddInt64(&s.SuccessRequests, 1)
	} else {
		atomic.AddInt64(&s.FailedRequests, 1)
	}
}

func (s *Stats) AddReadRequest() {
	atomic.AddInt64(&s.ReadRequests, 1)
	atomic.AddInt64(&s.TotalRequests, 1)
}

func (s *Stats) AddWriteRequest() {
	atomic.AddInt64(&s.WriteRequests, 1)
	atomic.AddInt64(&s.TotalRequests, 1)
}

func (s *Stats) CalculateStats() {
	s.EndTime = time.Now()
	duration := s.EndTime.Sub(s.StartTime).Seconds()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Sort latencies for percentile calculations
	sort.Slice(s.Latencies, func(i, j int) bool {
		return s.Latencies[i] < s.Latencies[j]
	})

	// Calculate statistics
	s.RequestsPerSec = float64(s.TotalRequests) / duration

	if len(s.Latencies) > 0 {
		s.AverageLatency = float64(s.TotalLatency) / float64(len(s.Latencies)) / 1000.0 // Convert to ms

		// Calculate percentiles
		p50Index := int(float64(len(s.Latencies)) * 0.5)
		p90Index := int(float64(len(s.Latencies)) * 0.9)
		p95Index := int(float64(len(s.Latencies)) * 0.95)
		p99Index := int(float64(len(s.Latencies)) * 0.99)

		if p50Index < len(s.Latencies) {
			s.P50Latency = float64(s.Latencies[p50Index]) / 1000.0 // Convert to ms
		}
		if p90Index < len(s.Latencies) {
			s.P90Latency = float64(s.Latencies[p90Index]) / 1000.0 // Convert to ms
		}
		if p95Index < len(s.Latencies) {
			s.P95Latency = float64(s.Latencies[p95Index]) / 1000.0 // Convert to ms
		}
		if p99Index < len(s.Latencies) {
			s.P99Latency = float64(s.Latencies[p99Index]) / 1000.0 // Convert to ms
		}
	}

	s.ErrorRate = float64(s.FailedRequests) / float64(s.TotalRequests) * 100.0
}

func (s *Stats) PrintStats() {
	fmt.Println("\n=== Load Test Results ===")
	fmt.Printf("Duration: %.2f seconds\n", s.EndTime.Sub(s.StartTime).Seconds())
	fmt.Printf("Total Requests: %d\n", s.TotalRequests)
	fmt.Printf("Successful Requests: %d\n", s.SuccessRequests)
	fmt.Printf("Failed Requests: %d\n", s.FailedRequests)
	fmt.Printf("Read Requests: %d\n", s.ReadRequests)
	fmt.Printf("Write Requests: %d\n", s.WriteRequests)
	fmt.Printf("Requests/sec: %.2f\n", s.RequestsPerSec)
	fmt.Printf("Error Rate: %.2f%%\n", s.ErrorRate)

	fmt.Println("\n=== Latency (ms) ===")
	fmt.Printf("Min: %.2f\n", float64(s.MinLatency)/1000.0)
	fmt.Printf("Max: %.2f\n", float64(s.MaxLatency)/1000.0)
	fmt.Printf("Average: %.2f\n", s.AverageLatency)
	fmt.Printf("P50: %.2f\n", s.P50Latency)
	fmt.Printf("P90: %.2f\n", s.P90Latency)
	fmt.Printf("P95: %.2f\n", s.P95Latency)
	fmt.Printf("P99: %.2f\n", s.P99Latency)

	fmt.Println("\n=== Status Codes ===")
	for code, count := range s.StatusCodes {
		fmt.Printf("%d: %d\n", code, count)
	}
}

func (s *Stats) WriteToFile(filename string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write CSV header
	file.WriteString("metric,value\n")

	// Write metrics
	file.WriteString(fmt.Sprintf("duration_seconds,%.2f\n", s.EndTime.Sub(s.StartTime).Seconds()))
	file.WriteString(fmt.Sprintf("total_requests,%d\n", s.TotalRequests))
	file.WriteString(fmt.Sprintf("successful_requests,%d\n", s.SuccessRequests))
	file.WriteString(fmt.Sprintf("failed_requests,%d\n", s.FailedRequests))
	file.WriteString(fmt.Sprintf("read_requests,%d\n", s.ReadRequests))
	file.WriteString(fmt.Sprintf("write_requests,%d\n", s.WriteRequests))
	file.WriteString(fmt.Sprintf("requests_per_second,%.2f\n", s.RequestsPerSec))
	file.WriteString(fmt.Sprintf("error_rate,%.2f\n", s.ErrorRate))
	file.WriteString(fmt.Sprintf("min_latency_ms,%.2f\n", float64(s.MinLatency)/1000.0))
	file.WriteString(fmt.Sprintf("max_latency_ms,%.2f\n", float64(s.MaxLatency)/1000.0))
	file.WriteString(fmt.Sprintf("avg_latency_ms,%.2f\n", s.AverageLatency))
	file.WriteString(fmt.Sprintf("p50_latency_ms,%.2f\n", s.P50Latency))
	file.WriteString(fmt.Sprintf("p90_latency_ms,%.2f\n", s.P90Latency))
	file.WriteString(fmt.Sprintf("p95_latency_ms,%.2f\n", s.P95Latency))
	file.WriteString(fmt.Sprintf("p99_latency_ms,%.2f\n", s.P99Latency))

	// Write status codes
	for code, count := range s.StatusCodes {
		file.WriteString(fmt.Sprintf("status_code_%d,%d\n", code, count))
	}

	return nil
}

// Helper functions
func generateRandomString(length int) string {
	buffer := make([]byte, int(math.Ceil(float64(length)/1.33333333333)))
	rand.Read(buffer)
	return base64.RawURLEncoding.EncodeToString(buffer)[:length]
}

func randomKey() string {
	return fmt.Sprintf("%s%d", keyPrefix, time.Now().UnixNano()%int64(keyCount))
}

func randomValue() string {
	randomPart := generateRandomString(valueSize - len(valuePrefix))
	return valuePrefix + randomPart
}

// HTTP client with timeout
var httpClient = &http.Client{
	Timeout: 10 * time.Second,
}

// Worker function
func worker(id int, stats *Stats, wg *sync.WaitGroup, throttle chan struct{}, done chan struct{}) {
	defer wg.Done()

	for {
		select {
		case <-done:
			return
		case <-throttle:
			// Determine if this is a read or write based on the ratio
			isRead := false
			if readWriteRatio > 0 {
				isRead = (time.Now().UnixNano() % 100) < int64(readWriteRatio*100)
			}

			var req *http.Request
			var err error

			key := randomKey()
			url := fmt.Sprintf("%s/kv/%s", targetURL, key)

			startTime := time.Now()

			if isRead {
				// GET request
				req, err = http.NewRequest("GET", url, nil)
				stats.AddReadRequest()
			} else {
				// PUT request
				value := randomValue()
				req, err = http.NewRequest("PUT", url, bytes.NewBufferString(value))
				stats.AddWriteRequest()
			}

			if err != nil {
				log.Printf("Error creating request: %v", err)
				stats.AddStatusCode(0)
				continue
			}

			resp, err := httpClient.Do(req)
			latency := time.Since(startTime)
			stats.AddLatency(latency)

			if err != nil {
				log.Printf("Error making request: %v", err)
				stats.AddStatusCode(0)
				continue
			}

			// Read and discard response body
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			stats.AddStatusCode(resp.StatusCode)
		}
	}
}

// Progress reporter
func reportProgress(stats *Stats, done chan struct{}) {
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	prevRequests := int64(0)

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			currentRequests := atomic.LoadInt64(&stats.TotalRequests)
			currentSuccessful := atomic.LoadInt64(&stats.SuccessRequests)
			currentFailed := atomic.LoadInt64(&stats.FailedRequests)

			reqPerSec := float64(currentRequests-prevRequests) / reportInterval.Seconds()

			fmt.Printf("[%s] Requests: %d (%.2f/sec), Success: %d, Failed: %d\n",
				time.Now().Format("15:04:05"),
				currentRequests,
				reqPerSec,
				currentSuccessful,
				currentFailed)

			prevRequests = currentRequests
		}
	}
}

// Warmup function
func warmup() {
	fmt.Printf("Warming up with %d keys for %s...\n", warmupKeyCount, warmupDuration)

	client := &http.Client{Timeout: 5 * time.Second}

	// Pre-populate some keys
	for i := 0; i < warmupKeyCount; i++ {
		key := fmt.Sprintf("%s%d", keyPrefix, i)
		value := valuePrefix + generateRandomString(valueSize-len(valuePrefix))
		url := fmt.Sprintf("%s/kv/%s", targetURL, key)

		req, err := http.NewRequest("PUT", url, bytes.NewBufferString(value))
		if err != nil {
			log.Printf("Error creating warmup request: %v", err)
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error making warmup request: %v", err)
			continue
		}

		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if i%10 == 0 {
			fmt.Printf("Warmed up %d/%d keys\n", i, warmupKeyCount)
		}
	}

	fmt.Println("Warmup complete, waiting for system to stabilize...")
	time.Sleep(warmupDuration)
}

func main() {
	// Parse command line flags
	flag.StringVar(&targetURL, "url", "http://localhost:8081", "Target URL of the hashmap server")
	flag.IntVar(&numThreads, "threads", 8, "Number of concurrent threads")
	flag.DurationVar(&duration, "duration", 30*time.Second, "Test duration")
	flag.DurationVar(&rampUpTime, "ramp-up", 5*time.Second, "Ramp-up time")
	flag.Float64Var(&readWriteRatio, "read-ratio", 0.8, "Read/write ratio (0.8 = 80% reads)")
	flag.IntVar(&keyCount, "keys", 10000, "Number of unique keys to use")
	flag.IntVar(&valueSize, "value-size", 100, "Size of each value in bytes")
	flag.DurationVar(&reportInterval, "report-interval", 1*time.Second, "Progress report interval")
	flag.StringVar(&keyPrefix, "key-prefix", "loadtest-key-", "Prefix for keys")
	flag.StringVar(&valuePrefix, "value-prefix", "loadtest-value-", "Prefix for values")
	flag.StringVar(&outputFile, "output", "loadtest-results.csv", "Output file for results")
	flag.IntVar(&requestsPerSec, "rps", 0, "Target requests per second (0 = unlimited)")
	flag.IntVar(&maxConcurrency, "max-concurrency", 0, "Maximum concurrency (0 = unlimited)")
	flag.DurationVar(&warmupDuration, "warmup-duration", 5*time.Second, "Warmup duration")
	flag.IntVar(&warmupKeyCount, "warmup-keys", 100, "Number of keys to pre-populate during warmup")

	flag.Parse()

	// Validate parameters
	if maxConcurrency == 0 {
		maxConcurrency = numThreads
	}

	// Print configuration
	fmt.Println("=== Load Test Configuration ===")
	fmt.Printf("Target URL: %s\n", targetURL)
	fmt.Printf("Threads: %d\n", numThreads)
	fmt.Printf("Duration: %s\n", duration)
	fmt.Printf("Ramp-up Time: %s\n", rampUpTime)
	fmt.Printf("Read/Write Ratio: %.2f\n", readWriteRatio)
	fmt.Printf("Key Count: %d\n", keyCount)
	fmt.Printf("Value Size: %d bytes\n", valueSize)
	fmt.Printf("Target RPS: %d\n", requestsPerSec)
	fmt.Printf("Max Concurrency: %d\n", maxConcurrency)
	fmt.Printf("Warmup Duration: %s\n", warmupDuration)
	fmt.Printf("Warmup Keys: %d\n", warmupKeyCount)

	// Perform warmup
	warmup()

	// Initialize statistics
	stats := NewStats()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create channels for coordination
	done := make(chan struct{})
	var throttle chan struct{}

	// Setup rate limiting if requested
	if requestsPerSec > 0 {
		throttle = make(chan struct{}, maxConcurrency)
		go func() {
			ticker := time.NewTicker(time.Second / time.Duration(requestsPerSec))
			defer ticker.Stop()

			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					select {
					case throttle <- struct{}{}:
					default:
						// Channel is full, skip this tick
					}
				}
			}
		}()
	} else {
		// No rate limiting, just fill the channel
		throttle = make(chan struct{}, maxConcurrency)
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					select {
					case throttle <- struct{}{}:
					default:
						// Channel is full, wait a bit
						time.Sleep(1 * time.Millisecond)
					}
				}
			}
		}()
	}

	// Start progress reporter
	go reportProgress(stats, done)

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go worker(i, stats, &wg, throttle, done)
	}

	// Setup test duration timer
	timer := time.NewTimer(duration)

	// Wait for duration or interrupt
	select {
	case <-timer.C:
		fmt.Println("\nTest duration completed")
	case sig := <-sigChan:
		fmt.Printf("\nReceived signal %v, stopping test\n", sig)
	}

	// Signal workers to stop
	close(done)

	// Wait for all workers to finish
	wg.Wait()

	// Calculate and print statistics
	stats.CalculateStats()
	stats.PrintStats()

	// Write results to file
	if err := stats.WriteToFile(outputFile); err != nil {
		log.Printf("Error writing results to file: %v", err)
	} else {
		fmt.Printf("\nResults written to %s\n", outputFile)
	}
}
