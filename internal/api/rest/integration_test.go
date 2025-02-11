//go:build integration
// +build integration

package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestIntegration_BasicOperations(t *testing.T) {
	baseURL := "http://localhost:8080"
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Test PUT operation
	t.Run("PUT operation", func(t *testing.T) {
		data := []byte("test-value")
		req, err := http.NewRequest(http.MethodPut, baseURL+"/keys/test-key", bytes.NewReader(data))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "text/plain")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %v, got %v", http.StatusCreated, resp.StatusCode)
		}
	})

	// Test GET operation
	t.Run("GET operation", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/keys/test-key")
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status %v, got %v", http.StatusOK, resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("Failed to read response body: %v", err)
		}

		if string(body) != "test-value" {
			t.Errorf("Expected body %q, got %q", "test-value", string(body))
		}
	})

	// Test DELETE operation
	t.Run("DELETE operation", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodDelete, baseURL+"/keys/test-key", nil)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("Expected status %v, got %v", http.StatusNoContent, resp.StatusCode)
		}

		// Verify key is deleted
		resp, err = http.Get(baseURL + "/keys/test-key")
		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status %v, got %v", http.StatusNotFound, resp.StatusCode)
		}
	})
}

func TestIntegration_ContentTypes(t *testing.T) {
	baseURL := "http://localhost:8080"
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	tests := []struct {
		name        string
		contentType string
		data        []byte
	}{
		{
			name:        "JSON data",
			contentType: "application/json",
			data:        []byte(`{"test":"value"}`),
		},
		{
			name:        "Text data",
			contentType: "text/plain",
			data:        []byte("Hello, World!"),
		},
		{
			name:        "Binary data",
			contentType: "application/octet-stream",
			data:        []byte{0x00, 0x01, 0x02, 0x03},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := fmt.Sprintf("test-key-%s", tt.name)

			// PUT request
			req, err := http.NewRequest(http.MethodPut, baseURL+"/keys/"+key, bytes.NewReader(tt.data))
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}
			req.Header.Set("Content-Type", tt.contentType)

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("Failed to make request: %v", err)
			}
			resp.Body.Close()

			if resp.StatusCode != http.StatusCreated {
				t.Errorf("Expected status %v, got %v", http.StatusCreated, resp.StatusCode)
			}

			// GET request
			resp, err = http.Get(baseURL + "/keys/" + key)
			if err != nil {
				t.Fatalf("Failed to make request: %v", err)
			}
			defer resp.Body.Close()

			if resp.Header.Get("Content-Type") != tt.contentType {
				t.Errorf("Expected Content-Type %s, got %s", tt.contentType, resp.Header.Get("Content-Type"))
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response body: %v", err)
			}

			if !bytes.Equal(body, tt.data) {
				t.Errorf("Response body doesn't match: expected %v, got %v", tt.data, body)
			}
		})
	}
}

func TestIntegration_Health(t *testing.T) {
	baseURL := "http://localhost:8080"
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status %v, got %v", http.StatusOK, resp.StatusCode)
	}

	if resp.Header.Get("Content-Type") != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", resp.Header.Get("Content-Type"))
	}

	var response struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Status != "ok" {
		t.Errorf("Expected status 'ok', got '%v'", response.Status)
	}
}

func TestIntegration_ErrorCases(t *testing.T) {
	baseURL := "http://localhost:8080"
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	tests := []struct {
		name           string
		method         string
		path           string
		body           []byte
		contentType    string
		expectedStatus int
	}{
		{
			name:           "GET non-existent key",
			method:         http.MethodGet,
			path:           "/keys/non-existent",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Invalid method",
			method:         http.MethodPatch,
			path:           "/keys/test",
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "Empty key",
			method:         http.MethodGet,
			path:           "/keys/",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Invalid JSON",
			method:         http.MethodPut,
			path:           "/keys/test",
			body:           []byte("{invalid json}"),
			contentType:    "application/json",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req *http.Request
			var err error

			if tt.body != nil {
				req, err = http.NewRequest(tt.method, baseURL+tt.path, bytes.NewReader(tt.body))
			} else {
				req, err = http.NewRequest(tt.method, baseURL+tt.path, nil)
			}
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("Failed to make request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status %v, got %v", tt.expectedStatus, resp.StatusCode)
			}
		})
	}
}
