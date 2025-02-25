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
	"runtime"
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
	keyCount       int
	valueSize      int
	reportInterval time.Duration
	keyPrefix      string
	valuePrefix    string
	outputFile     string
	maxKeys        int
	batchSize      int
)

// Statistics
type Stats struct {
	TotalRequests   int64
	SuccessRequests int64
	FailedRequests  int64
	KeysStored      int64
	BytesStored     int64
	StatusCodes     map[int]int64
	StartTime       time.Time
	EndTime         time.Time
	mu              sync.Mutex
}

func NewStats() *Stats {
	return &Stats{
		StatusCodes: make(map[int]int64),
		StartTime:   time.Now(),
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

func (s *Stats) AddRequest() {
	atomic.AddInt64(&s.TotalRequests, 1)
}

func (s *Stats) AddKeyStored() {
	atomic.AddInt64(&s.KeysStored, 1)
}

func (s *Stats) AddBytesStored(bytes int64) {
	atomic.AddInt64(&s.BytesStored, bytes)
}

func (s *Stats) CalculateStats() {
	s.EndTime = time.Now()
}

func (s *Stats) PrintStats() {
	duration := s.EndTime.Sub(s.StartTime).Seconds()

	fmt.Println("\n=== Stress Test Results ===")
	fmt.Printf("Duration: %.2f seconds\n", duration)
	fmt.Printf("Total Requests: %d\n", s.TotalRequests)
	fmt.Printf("Successful Requests: %d\n", s.SuccessRequests)
	fmt.Printf("Failed Requests: %d\n", s.FailedRequests)
	fmt.Printf("Keys Stored: %d\n", s.KeysStored)
	fmt.Printf("Data Stored: %.2f MB\n", float64(s.BytesStored)/(1024*1024))
	fmt.Printf("Requests/sec: %.2f\n", float64(s.TotalRequests)/duration)
	fmt.Printf("Storage Rate: %.2f MB/sec\n", float64(s.BytesStored)/(1024*1024)/duration)

	fmt.Println("\n=== Status Codes ===")
	for code, count := range s.StatusCodes {
		fmt.Printf("%d: %d\n", code, count)
	}
}

func (s *Stats) WriteToFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	duration := s.EndTime.Sub(s.StartTime).Seconds()

	// Write CSV header
	file.WriteString("metric,value\n")

	// Write metrics
	file.WriteString(fmt.Sprintf("duration_seconds,%.2f\n", duration))
	file.WriteString(fmt.Sprintf("total_requests,%d\n", s.TotalRequests))
	file.WriteString(fmt.Sprintf("successful_requests,%d\n", s.SuccessRequests))
	file.WriteString(fmt.Sprintf("failed_requests,%d\n", s.FailedRequests))
	file.WriteString(fmt.Sprintf("keys_stored,%d\n", s.KeysStored))
	file.WriteString(fmt.Sprintf("bytes_stored,%d\n", s.BytesStored))
	file.WriteString(fmt.Sprintf("requests_per_second,%.2f\n", float64(s.TotalRequests)/duration))
	file.WriteString(fmt.Sprintf("storage_rate_mb_per_second,%.2f\n", float64(s.BytesStored)/(1024*1024)/duration))

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

// HTTP client with timeout
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
}

// Worker function
func worker(id int, stats *Stats, wg *sync.WaitGroup, keyCounter *int64, done chan struct{}) {
	defer wg.Done()

	for {
		select {
		case <-done:
			return
		default:
			// Check if we've reached the maximum number of keys
			currentKey := atomic.AddInt64(keyCounter, 1)
			if maxKeys > 0 && currentKey > int64(maxKeys) {
				return
			}

			// Generate key and value
			key := fmt.Sprintf("%s%d", keyPrefix, currentKey)
			value := valuePrefix + generateRandomString(valueSize-len(valuePrefix))
			url := fmt.Sprintf("%s/kv/%s", targetURL, key)

			// Create PUT request
			req, err := http.NewRequest("PUT", url, bytes.NewBufferString(value))
			if err != nil {
				log.Printf("Error creating request: %v", err)
				continue
			}

			stats.AddRequest()

			// Send request
			resp, err := httpClient.Do(req)
			if err != nil {
				log.Printf("Error making request: %v", err)
				stats.AddStatusCode(0)
				continue
			}

			// Read and discard response body
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			stats.AddStatusCode(resp.StatusCode)

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				stats.AddKeyStored()
				stats.AddBytesStored(int64(len(value)))
			}
		}
	}
}

// Progress reporter
func reportProgress(stats *Stats, keyCounter *int64, done chan struct{}) {
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	var prevRequests int64
	var prevBytes int64
	var prevKeys int64

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			currentRequests := atomic.LoadInt64(&stats.TotalRequests)
			currentSuccessful := atomic.LoadInt64(&stats.SuccessRequests)
			currentFailed := atomic.LoadInt64(&stats.FailedRequests)
			currentKeys := atomic.LoadInt64(&stats.KeysStored)
			currentBytes := atomic.LoadInt64(&stats.BytesStored)
			currentKeyCounter := atomic.LoadInt64(keyCounter)

			reqPerSec := float64(currentRequests-prevRequests) / reportInterval.Seconds()
			keysPerSec := float64(currentKeys-prevKeys) / reportInterval.Seconds()
			mbPerSec := float64(currentBytes-prevBytes) / (1024 * 1024) / reportInterval.Seconds()

			// Get memory stats
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			fmt.Printf("[%s] Keys: %d/%d, Reqs: %d (%.2f/sec), Success: %d, Failed: %d, Data: %.2f MB (%.2f MB/sec), Mem: %.2f MB\n",
				time.Now().Format("15:04:05"),
				currentKeys,
				currentKeyCounter,
				currentRequests,
				reqPerSec,
				currentSuccessful,
				currentFailed,
				float64(currentBytes)/(1024*1024),
				mbPerSec,
				float64(m.Alloc)/(1024*1024))

			prevRequests = currentRequests
			prevKeys = currentKeys
			prevBytes = currentBytes
		}
	}
}

// Batch worker function for faster loading
func batchWorker(id int, stats *Stats, wg *sync.WaitGroup, keyCounter *int64, done chan struct{}) {
	defer wg.Done()

	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	for {
		select {
		case <-done:
			return
		default:
			// Process a batch of keys
			for i := 0; i < batchSize; i++ {
				// Check if we've reached the maximum number of keys
				currentKey := atomic.AddInt64(keyCounter, 1)
				if maxKeys > 0 && currentKey > int64(maxKeys) {
					return
				}

				// Generate key and value
				key := fmt.Sprintf("%s%d", keyPrefix, currentKey)
				value := valuePrefix + generateRandomString(valueSize-len(valuePrefix))
				url := fmt.Sprintf("%s/kv/%s", targetURL, key)

				// Create PUT request
				req, err := http.NewRequest("PUT", url, bytes.NewBufferString(value))
				if err != nil {
					log.Printf("Error creating request: %v", err)
					continue
				}

				stats.AddRequest()

				// Send request
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("Error making request: %v", err)
					stats.AddStatusCode(0)
					continue
				}

				// Read and discard response body
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				stats.AddStatusCode(resp.StatusCode)

				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					stats.AddKeyStored()
					stats.AddBytesStored(int64(len(value)))
				}
			}
		}
	}
}

func main() {
	// Parse command line flags
	flag.StringVar(&targetURL, "url", "http://localhost:8081", "Target URL of the hashmap server")
	flag.IntVar(&numThreads, "threads", 8, "Number of concurrent threads")
	flag.DurationVar(&duration, "duration", 5*time.Minute, "Test duration")
	flag.IntVar(&keyCount, "keys", 100000, "Initial number of keys to use")
	flag.IntVar(&valueSize, "value-size", 1024, "Size of each value in bytes")
	flag.DurationVar(&reportInterval, "report-interval", 5*time.Second, "Progress report interval")
	flag.StringVar(&keyPrefix, "key-prefix", "stress-key-", "Prefix for keys")
	flag.StringVar(&valuePrefix, "value-prefix", "stress-value-", "Prefix for values")
	flag.StringVar(&outputFile, "output", "stress-results.csv", "Output file for results")
	flag.IntVar(&maxKeys, "max-keys", 0, "Maximum number of keys to store (0 = unlimited)")
	flag.IntVar(&batchSize, "batch-size", 10, "Number of keys to process in each batch")

	flag.Parse()

	// Print configuration
	fmt.Println("=== Stress Test Configuration ===")
	fmt.Printf("Target URL: %s\n", targetURL)
	fmt.Printf("Threads: %d\n", numThreads)
	fmt.Printf("Duration: %s\n", duration)
	fmt.Printf("Key Count: %d\n", keyCount)
	fmt.Printf("Value Size: %d bytes\n", valueSize)
	fmt.Printf("Max Keys: %d\n", maxKeys)
	fmt.Printf("Batch Size: %d\n", batchSize)
	fmt.Printf("Estimated Data Size: %.2f MB\n", float64(maxKeys*valueSize)/(1024*1024))

	// Initialize statistics
	stats := NewStats()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create channels for coordination
	done := make(chan struct{})

	// Initialize key counter
	var keyCounter int64

	// Start progress reporter
	go reportProgress(stats, &keyCounter, done)

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go batchWorker(i, stats, &wg, &keyCounter, done)
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
