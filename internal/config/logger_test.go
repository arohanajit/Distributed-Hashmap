package config

import (
	"os"
	"testing"

	"go.uber.org/zap"
)

func TestInitLogger(t *testing.T) {
	// Save original log level
	origLogLevel := os.Getenv("LOG_LEVEL")
	defer os.Setenv("LOG_LEVEL", origLogLevel)

	tests := []struct {
		name     string
		logLevel string
		wantErr  bool
	}{
		{
			name:     "debug level",
			logLevel: "debug",
			wantErr:  false,
		},
		{
			name:     "info level",
			logLevel: "info",
			wantErr:  false,
		},
		{
			name:     "invalid level",
			logLevel: "invalid",
			wantErr:  false, // Should not error, just use default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset logger
			Logger = nil

			// Set log level
			os.Setenv("LOG_LEVEL", tt.logLevel)

			// Initialize logger
			err := InitLogger()
			if (err != nil) != tt.wantErr {
				t.Errorf("InitLogger() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if Logger == nil {
				t.Error("Logger should not be nil after initialization")
				return
			}

			// Test logging
			Logger.Info("test message",
				zap.String("test_field", "test_value"),
			)
		})
	}
}

func TestGetLogger(t *testing.T) {
	// Reset logger
	Logger = nil

	// Get logger when not initialized
	logger := GetLogger()
	if logger == nil {
		t.Error("GetLogger() should never return nil")
	}

	// Initialize logger properly
	err := InitLogger()
	if err != nil {
		t.Errorf("InitLogger() failed: %v", err)
	}

	// Get initialized logger
	logger = GetLogger()
	if logger == nil {
		t.Error("GetLogger() should never return nil after initialization")
	}
}

func TestSync(t *testing.T) {
	// Test with nil logger
	Logger = nil
	if err := Sync(); err != nil {
		t.Errorf("Sync() with nil logger should not return error, got %v", err)
	}

	// Test with initialized logger
	err := InitLogger()
	if err != nil {
		t.Errorf("InitLogger() failed: %v", err)
	}

	if err := Sync(); err != nil {
		// Note: zap's sync can return error on some platforms, so we don't treat it as a test failure
		t.Logf("Sync() returned error: %v", err)
	}
}
