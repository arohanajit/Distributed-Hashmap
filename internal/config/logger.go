package config

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// Logger is the global logger instance
	Logger *zap.Logger
)

// InitLogger initializes the global logger
func InitLogger() error {
	config := zap.NewProductionConfig()

	// Customize the logging format
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.StacktraceKey = "" // Disable stacktrace by default

	// Set log level based on environment
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel != "" {
		var level zapcore.Level
		if err := level.UnmarshalText([]byte(logLevel)); err == nil {
			config.Level.SetLevel(level)
		}
	}

	// Create the logger
	var err error
	Logger, err = config.Build(
		zap.AddCallerSkip(1),
		zap.AddCaller(),
	)
	if err != nil {
		return err
	}

	// Replace the global logger
	zap.ReplaceGlobals(Logger)

	return nil
}

// GetLogger returns the global logger instance
func GetLogger() *zap.Logger {
	if Logger == nil {
		// If logger is not initialized, create a default logger
		Logger = zap.NewExample()
		zap.ReplaceGlobals(Logger)
	}
	return Logger
}

// Sync flushes any buffered log entries
func Sync() error {
	if Logger != nil {
		return Logger.Sync()
	}
	return nil
}
