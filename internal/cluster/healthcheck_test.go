package cluster

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGossipProtocol_BasicOperations(t *testing.T) {
	fd := NewFailureDetector(time.Second, 2)
	gp := NewGossipProtocol("local-node", fd)

	// Test adding nodes
	gp.AddNode("node1", "localhost:8081")
	gp.AddNode("node2", "localhost:8082")

	// Test removing nodes
	gp.RemoveNode("node1")

	// Verify node removal
	suspected := gp.GetSuspectedNodes()
	if len(suspected) != 0 {
		t.Errorf("Expected 0 suspected nodes, got %d", len(suspected))
	}
}

func TestGossipProtocol_MessageHandling(t *testing.T) {
	fd := NewFailureDetector(time.Second, 2)
	gp := NewGossipProtocol("local-node", fd)

	// Create test message
	message := GossipMessage{
		SenderID:  "test-node",
		Timestamp: time.Now(),
		HealthStatus: map[string]NodeHealth{
			"node1": {
				LastHeartbeat: time.Now(),
				MissedBeats:   0,
				IsHealthy:     true,
			},
		},
		Suspected: []string{"node2"},
	}

	// Handle message
	gp.HandleGossipMessage(message)

	// Verify suspected nodes
	suspected := gp.GetSuspectedNodes()
	if len(suspected) != 1 || suspected[0] != "node2" {
		t.Error("Expected node2 to be marked as suspected")
	}
}

func TestGossipProtocol_NetworkCommunication(t *testing.T) {
	// Create test server
	messageReceived := make(chan GossipMessage, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cluster/gossip" {
			var message GossipMessage
			if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
				t.Errorf("Failed to decode gossip message: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			messageReceived <- message
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	fd := NewFailureDetector(time.Second, 2)
	gp := NewGossipProtocol("local-node", fd)

	// Add test node
	gp.AddNode("test-node", server.URL[7:]) // Strip "http://"

	// Create and send test message
	message := gp.createGossipMessage()
	gp.sendGossipMessage("test-node", message)

	// Wait for message to be received
	select {
	case received := <-messageReceived:
		if received.SenderID != "local-node" {
			t.Errorf("Expected sender ID 'local-node', got '%s'", received.SenderID)
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for gossip message")
	}
}

func TestGossipProtocol_SuspectedNodesCleanup(t *testing.T) {
	fd := NewFailureDetector(100*time.Millisecond, 2)
	gp := NewGossipProtocol("local-node", fd)

	// Add test nodes
	gp.AddNode("node1", "localhost:8081")
	gp.AddNode("node2", "localhost:8082")

	// Mark nodes as suspected
	gp.markNodeSuspected("node1")
	gp.markNodeSuspected("node2")

	// Verify initial suspected nodes
	suspected := gp.GetSuspectedNodes()
	if len(suspected) != 2 {
		t.Errorf("Expected 2 suspected nodes, got %d", len(suspected))
	}

	// Wait for TTL to expire
	time.Sleep(suspectTTL + 100*time.Millisecond)

	// Force cleanup
	gp.ForceCleanup()

	// Verify cleanup
	suspected = gp.GetSuspectedNodes()
	if len(suspected) != 0 {
		t.Errorf("Expected 0 suspected nodes after cleanup, got %d", len(suspected))
	}
}

func TestGossipProtocol_ConcurrentAccess(t *testing.T) {
	fd := NewFailureDetector(100*time.Millisecond, 2)
	gp := NewGossipProtocol("local-node", fd)
	done := make(chan bool)

	// Start concurrent operations
	go func() {
		for i := 0; i < 100; i++ {
			gp.AddNode("node1", "localhost:8081")
			gp.markNodeSuspected("node1")
			gp.RemoveNode("node1")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			gp.GetSuspectedNodes()
			gp.createGossipMessage()
		}
		done <- true
	}()

	// Wait for operations to complete
	<-done
	<-done
}
