package config

import (
	"os"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Port != 8080 {
		t.Errorf("Expected default port 8080, got %d", cfg.Port)
	}
	if cfg.Host != "0.0.0.0" {
		t.Errorf("Expected default host 0.0.0.0, got %s", cfg.Host)
	}
	if cfg.MaxPayloadSize != 1024*1024 {
		t.Errorf("Expected default max payload size 1MB, got %d", cfg.MaxPayloadSize)
	}
}

func TestLoadConfig(t *testing.T) {
	// Save original env vars
	origPort := os.Getenv("SERVER_PORT")
	origHost := os.Getenv("SERVER_HOST")
	defer func() {
		os.Setenv("SERVER_PORT", origPort)
		os.Setenv("SERVER_HOST", origHost)
	}()

	// Test environment variable overrides
	tests := []struct {
		name     string
		envVars  map[string]string
		validate func(*ServerConfig) error
	}{
		{
			name: "custom port and host",
			envVars: map[string]string{
				"SERVER_PORT": "9090",
				"SERVER_HOST": "127.0.0.1",
			},
			validate: func(cfg *ServerConfig) error {
				if cfg.Port != 9090 {
					t.Errorf("Expected port 9090, got %d", cfg.Port)
				}
				if cfg.Host != "127.0.0.1" {
					t.Errorf("Expected host 127.0.0.1, got %s", cfg.Host)
				}
				return nil
			},
		},
		{
			name: "custom shard configuration",
			envVars: map[string]string{
				"SHARD_COUNT":        "8",
				"REPLICATION_FACTOR": "3",
			},
			validate: func(cfg *ServerConfig) error {
				if cfg.ShardCount != 8 {
					t.Errorf("Expected shard count 8, got %d", cfg.ShardCount)
				}
				if cfg.ReplicationFactor != 3 {
					t.Errorf("Expected replication factor 3, got %d", cfg.ReplicationFactor)
				}
				return nil
			},
		},
		{
			name: "invalid port value",
			envVars: map[string]string{
				"SERVER_PORT": "invalid",
			},
			validate: func(cfg *ServerConfig) error {
				if cfg.Port != 8080 {
					t.Errorf("Expected default port 8080 for invalid input, got %d", cfg.Port)
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}

			cfg := LoadConfig()
			if err := tt.validate(cfg); err != nil {
				t.Error(err)
			}

			// Clean up environment variables
			for k := range tt.envVars {
				os.Unsetenv(k)
			}
		})
	}
}
