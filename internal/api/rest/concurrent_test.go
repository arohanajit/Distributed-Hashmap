package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestConcurrentAccess(t *testing.T) {
	// Create a test server
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate some work
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	// Test parameters
	concurrentRequests := 10
	var wg sync.WaitGroup
	errorChan := make(chan error, concurrentRequests)

	// Start concurrent requests
	for i := 0; i < concurrentRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Prepare request data
			data := map[string]string{
				"key":   fmt.Sprintf("key-%d", id),
				"value": fmt.Sprintf("value-%d", id),
			}
			jsonData, err := json.Marshal(data)
			if err != nil {
				errorChan <- fmt.Errorf("failed to marshal data for request %d: %v", id, err)
				return
			}

			// Make request
			resp, err := http.Post(server.URL+"/keys", "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				errorChan <- fmt.Errorf("request %d failed: %v", id, err)
				return
			}
			defer resp.Body.Close()

			// Check response
			if resp.StatusCode != http.StatusOK {
				errorChan <- fmt.Errorf("request %d got unexpected status: %d", id, resp.StatusCode)
				return
			}
		}(i)
	}

	// Wait for all requests to complete
	wg.Wait()
	close(errorChan)

	// Check for any errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		for _, err := range errors {
			t.Error(err)
		}
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	// Create a test server with a simple in-memory store
	store := make(map[string]string)
	var storeMutex sync.RWMutex

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			key := r.URL.Path[6:] // Strip "/keys/"
			storeMutex.RLock()
			value, exists := store[key]
			storeMutex.RUnlock()

			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Write([]byte(value))

		case http.MethodPut:
			var data map[string]string
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			storeMutex.Lock()
			store[data["key"]] = data["value"]
			storeMutex.Unlock()
			w.WriteHeader(http.StatusOK)
		}
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	// Test parameters
	readers := 5
	writers := 3
	iterations := 100
	var wg sync.WaitGroup
	errorChan := make(chan error, (readers+writers)*iterations)

	// Start writers
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := map[string]string{
					"key":   fmt.Sprintf("key-%d-%d", writerID, i),
					"value": fmt.Sprintf("value-%d-%d", writerID, i),
				}
				jsonData, err := json.Marshal(data)
				if err != nil {
					errorChan <- fmt.Errorf("writer %d failed to marshal data: %v", writerID, err)
					continue
				}

				resp, err := http.Post(server.URL+"/keys", "application/json", bytes.NewBuffer(jsonData))
				if err != nil {
					errorChan <- fmt.Errorf("writer %d request failed: %v", writerID, err)
					continue
				}
				resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					errorChan <- fmt.Errorf("writer %d got unexpected status: %d", writerID, resp.StatusCode)
				}
			}
		}(w)
	}

	// Start readers
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// Read a random key from any writer
				writerID := i % writers
				keyID := i % iterations
				key := fmt.Sprintf("key-%d-%d", writerID, keyID)

				resp, err := http.Get(server.URL + "/keys/" + key)
				if err != nil {
					errorChan <- fmt.Errorf("reader %d request failed: %v", readerID, err)
					continue
				}
				resp.Body.Close()

				if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
					errorChan <- fmt.Errorf("reader %d got unexpected status: %d", readerID, resp.StatusCode)
				}
			}
		}(r)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errorChan)

	// Check for any errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		for _, err := range errors {
			t.Error(err)
		}
	}
}
