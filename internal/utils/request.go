package utils

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// GenerateRequestID generates a unique request ID for idempotency
// It can either use a cryptographically secure random number or a timestamp-based approach
func GenerateRequestID() string {
	// Try crypto random first
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err == nil {
		return hex.EncodeToString(bytes)
	}

	// Fallback to timestamp if crypto random fails
	return fmt.Sprintf("req-%d", time.Now().UnixNano())
}
