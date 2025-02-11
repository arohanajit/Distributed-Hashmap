package storage

import (
	"testing"
	"time"
)

func TestReplicaManager_BasicOperations(t *testing.T) {
	rm := NewReplicaManager(3, 2) // 3 replicas, quorum of 2
	nodes := []string{"node1", "node2", "node3"}
	key := "test-key"
	requestID := "test-request-1"

	// Test initialization
	tracker := rm.InitReplication(key, requestID, nodes)
	if tracker == nil {
		t.Fatal("Expected non-nil tracker")
	}

	if len(tracker.Replicas) != len(nodes) {
		t.Errorf("Expected %d replicas, got %d", len(nodes), len(tracker.Replicas))
	}

	// Test replica status updates
	err := rm.UpdateReplicaStatus(key, "node1", ReplicaSuccess)
	if err != nil {
		t.Errorf("Failed to update replica status: %v", err)
	}

	err = rm.UpdateReplicaStatus(key, "node2", ReplicaSuccess)
	if err != nil {
		t.Errorf("Failed to update replica status: %v", err)
	}

	// Test quorum check
	quorumMet, err := rm.CheckQuorum(key)
	if err != nil {
		t.Errorf("Failed to check quorum: %v", err)
	}
	if !quorumMet {
		t.Error("Expected quorum to be met")
	}

	// Test status retrieval
	status, err := rm.GetReplicaStatus(key)
	if err != nil {
		t.Errorf("Failed to get replica status: %v", err)
	}
	if status["node1"] != ReplicaSuccess {
		t.Errorf("Expected node1 status to be Success, got %v", status["node1"])
	}

	// Test cleanup
	rm.CleanupReplication(key)
	_, err = rm.GetReplicaStatus(key)
	if err == nil {
		t.Error("Expected error after cleanup, got nil")
	}
}

func TestReplicaManager_ErrorCases(t *testing.T) {
	rm := NewReplicaManager(3, 2)
	key := "test-key"

	// Test operations on non-existent key
	_, err := rm.GetReplicaStatus(key)
	if err == nil {
		t.Error("Expected error for non-existent key")
	}

	err = rm.UpdateReplicaStatus(key, "node1", ReplicaSuccess)
	if err == nil {
		t.Error("Expected error updating non-existent key")
	}

	quorumMet, err := rm.CheckQuorum(key)
	if err == nil {
		t.Error("Expected error checking quorum for non-existent key")
	}
	if quorumMet {
		t.Error("Expected quorum check to fail for non-existent key")
	}
}

func TestReplicaManager_BackoffDuration(t *testing.T) {
	rm := NewReplicaManager(3, 2)

	tests := []struct {
		name        string
		retryCount  int
		minExpected time.Duration
		maxExpected time.Duration
	}{
		{
			name:        "First retry",
			retryCount:  0,
			minExpected: time.Second,
			maxExpected: 2 * time.Second,
		},
		{
			name:        "Second retry",
			retryCount:  1,
			minExpected: 2 * time.Second,
			maxExpected: 4 * time.Second,
		},
		{
			name:        "Max backoff",
			retryCount:  10,
			minExpected: time.Minute,
			maxExpected: time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration := rm.GetBackoffDuration(tt.retryCount)
			if duration < tt.minExpected || duration > tt.maxExpected {
				t.Errorf("Expected backoff duration between %v and %v, got %v",
					tt.minExpected, tt.maxExpected, duration)
			}
		})
	}
}

func TestReplicaManager_ConcurrentOperations(t *testing.T) {
	rm := NewReplicaManager(3, 2)
	key := "test-key"
	requestID := "test-request-1"
	nodes := []string{"node1", "node2", "node3"}

	// Initialize replication
	rm.InitReplication(key, requestID, nodes)

	// Perform concurrent status updates
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			rm.UpdateReplicaStatus(key, "node1", ReplicaSuccess)
			rm.UpdateReplicaStatus(key, "node1", ReplicaPending)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			rm.GetReplicaStatus(key)
			rm.CheckQuorum(key)
		}
		done <- true
	}()

	// Wait for goroutines to complete
	<-done
	<-done

	// Verify final state is consistent
	status, err := rm.GetReplicaStatus(key)
	if err != nil {
		t.Errorf("Failed to get final status: %v", err)
	}
	if _, exists := status["node1"]; !exists {
		t.Error("Node1 status missing after concurrent operations")
	}
}
