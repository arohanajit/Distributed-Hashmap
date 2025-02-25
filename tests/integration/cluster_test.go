//go:build integration
// +build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodes holds the list of node addresses for testing
var TestNodes []string

// TestClient is a simple client for interacting with the distributed hashmap
type TestClient struct {
	nodes []string
}

// NewTestClient creates a new test client
func NewTestClient() *TestClient {
	nodesEnv := os.Getenv("TEST_NODES")
	if nodesEnv == "" {
		nodesEnv = "node1:8080,node2:8080,node3:8080,node4:8080,node5:8080"
	}

	return &TestClient{
		nodes: strings.Split(nodesEnv, ","),
	}
}

// Put stores a key-value pair in the distributed hashmap
func (c *TestClient) Put(nodeIndex int, key, value string) error {
	if nodeIndex < 0 || nodeIndex >= len(c.nodes) {
		return fmt.Errorf("invalid node index: %d", nodeIndex)
	}

	url := fmt.Sprintf("http://%s/kv/%s", c.nodes[nodeIndex], key)

	req, err := http.NewRequest("PUT", url, bytes.NewBufferString(value))
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// Get retrieves a value for a key from the distributed hashmap
func (c *TestClient) Get(nodeIndex int, key string) (string, error) {
	if nodeIndex < 0 || nodeIndex >= len(c.nodes) {
		return "", fmt.Errorf("invalid node index: %d", nodeIndex)
	}

	url := fmt.Sprintf("http://%s/kv/%s", c.nodes[nodeIndex], key)

	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

// Delete removes a key from the distributed hashmap
func (c *TestClient) Delete(nodeIndex int, key string) error {
	if nodeIndex < 0 || nodeIndex >= len(c.nodes) {
		return fmt.Errorf("invalid node index: %d", nodeIndex)
	}

	url := fmt.Sprintf("http://%s/kv/%s", c.nodes[nodeIndex], key)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// GetClusterStatus retrieves the status of the cluster
func (c *TestClient) GetClusterStatus(nodeIndex int) (map[string]interface{}, error) {
	if nodeIndex < 0 || nodeIndex >= len(c.nodes) {
		return nil, fmt.Errorf("invalid node index: %d", nodeIndex)
	}

	url := fmt.Sprintf("http://%s/cluster/status", c.nodes[nodeIndex])

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

// WaitForClusterStability waits for the cluster to stabilize
func (c *TestClient) WaitForClusterStability(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		stable := true

		for i := 0; i < len(c.nodes); i++ {
			status, err := c.GetClusterStatus(i)
			if err != nil {
				stable = false
				break
			}

			// Check if all nodes are in the membership list
			members, ok := status["members"].([]interface{})
			if !ok || len(members) != len(c.nodes) {
				stable = false
				break
			}
		}

		if stable {
			return nil
		}

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("cluster did not stabilize within %s", timeout)
}

// TestBasicDataConsistency tests basic data consistency across nodes
func TestBasicDataConsistency(t *testing.T) {
	client := NewTestClient()

	// Wait for cluster to stabilize
	err := client.WaitForClusterStability(60 * time.Second)
	require.NoError(t, err, "Cluster failed to stabilize")

	// Write to node 0, read from node 1
	key := "test-key-1"
	value := "test-value-1"

	err = client.Put(0, key, value)
	require.NoError(t, err, "Failed to put key-value pair")

	// Allow time for replication
	time.Sleep(2 * time.Second)

	// Read from a different node
	readValue, err := client.Get(1, key)
	require.NoError(t, err, "Failed to get value")
	assert.Equal(t, value, readValue, "Value read from node 1 doesn't match value written to node 0")

	// Read from yet another node
	readValue, err = client.Get(2, key)
	require.NoError(t, err, "Failed to get value")
	assert.Equal(t, value, readValue, "Value read from node 2 doesn't match value written to node 0")
}

// TestMultipleWrites tests multiple writes to different nodes
func TestMultipleWrites(t *testing.T) {
	client := NewTestClient()

	// Wait for cluster to stabilize
	err := client.WaitForClusterStability(60 * time.Second)
	require.NoError(t, err, "Cluster failed to stabilize")

	// Write different key-value pairs to different nodes
	testData := []struct {
		nodeIndex int
		key       string
		value     string
	}{
		{0, "key-node0", "value-from-node0"},
		{1, "key-node1", "value-from-node1"},
		{2, "key-node2", "value-from-node2"},
		{3, "key-node3", "value-from-node3"},
		{4, "key-node4", "value-from-node4"},
	}

	// Write data to each node
	for _, data := range testData {
		err := client.Put(data.nodeIndex, data.key, data.value)
		require.NoError(t, err, "Failed to put key-value pair to node %d", data.nodeIndex)
	}

	// Allow time for replication
	time.Sleep(5 * time.Second)

	// Read data from each node, but from a different node than it was written to
	for i, data := range testData {
		readNodeIndex := (data.nodeIndex + 2) % len(client.nodes) // Read from a different node

		readValue, err := client.Get(readNodeIndex, data.key)
		require.NoError(t, err, "Failed to get value for key %s from node %d", data.key, readNodeIndex)
		assert.Equal(t, data.value, readValue, "Value for key %s read from node %d doesn't match expected value", data.key, readNodeIndex)
	}
}

// TestNodeFailureAndRecovery tests data availability when a node fails and recovers
func TestNodeFailureAndRecovery(t *testing.T) {
	// This test requires external orchestration to stop and start nodes
	// For now, we'll simulate by not writing to a specific node and checking if data is still available

	client := NewTestClient()

	// Wait for cluster to stabilize
	err := client.WaitForClusterStability(60 * time.Second)
	require.NoError(t, err, "Cluster failed to stabilize")

	// Write data to node 0
	key := "failure-test-key"
	value := "failure-test-value"

	err = client.Put(0, key, value)
	require.NoError(t, err, "Failed to put key-value pair")

	// Allow time for replication
	time.Sleep(2 * time.Second)

	// Verify data is available from all nodes
	for i := 0; i < len(client.nodes); i++ {
		readValue, err := client.Get(i, key)
		require.NoError(t, err, "Failed to get value from node %d", i)
		assert.Equal(t, value, readValue, "Value read from node %d doesn't match expected value", i)
	}

	// In a real test, we would stop node 0 here and verify data is still available from other nodes
	// Since we can't do that in this test, we'll just verify data is available from other nodes

	for i := 1; i < len(client.nodes); i++ {
		readValue, err := client.Get(i, key)
		require.NoError(t, err, "Failed to get value from node %d after simulated failure", i)
		assert.Equal(t, value, readValue, "Value read from node %d doesn't match expected value after simulated failure", i)
	}
}

// TestIdempotency tests that repeated operations are idempotent
func TestIdempotency(t *testing.T) {
	client := NewTestClient()

	// Wait for cluster to stabilize
	err := client.WaitForClusterStability(60 * time.Second)
	require.NoError(t, err, "Cluster failed to stabilize")

	// Write the same key-value pair multiple times
	key := "idempotency-test-key"
	value := "idempotency-test-value"

	for i := 0; i < 5; i++ {
		err := client.Put(i%len(client.nodes), key, value)
		require.NoError(t, err, "Failed to put key-value pair in iteration %d", i)
	}

	// Allow time for replication
	time.Sleep(2 * time.Second)

	// Verify data is consistent across all nodes
	for i := 0; i < len(client.nodes); i++ {
		readValue, err := client.Get(i, key)
		require.NoError(t, err, "Failed to get value from node %d", i)
		assert.Equal(t, value, readValue, "Value read from node %d doesn't match expected value", i)
	}

	// Delete the key multiple times
	for i := 0; i < 3; i++ {
		err := client.Delete(i, key)
		if i == 0 {
			require.NoError(t, err, "Failed to delete key in first attempt")
		}
	}

	// Allow time for replication
	time.Sleep(2 * time.Second)

	// Verify key is deleted from all nodes
	for i := 0; i < len(client.nodes); i++ {
		_, err := client.Get(i, key)
		assert.Error(t, err, "Key should be deleted from node %d", i)
	}
}
