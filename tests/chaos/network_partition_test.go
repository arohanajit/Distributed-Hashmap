//go:build chaos
// +build chaos

package chaos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ChaosClient is a client for interacting with the distributed hashmap and injecting chaos
type ChaosClient struct {
	nodes []string
}

// NewChaosClient creates a new chaos client
func NewChaosClient() *ChaosClient {
	nodesEnv := os.Getenv("TEST_NODES")
	if nodesEnv == "" {
		nodesEnv = "node1:8080,node2:8080,node3:8080,node4:8080,node5:8080"
	}

	return &ChaosClient{
		nodes: strings.Split(nodesEnv, ","),
	}
}

// Put stores a key-value pair in the distributed hashmap
func (c *ChaosClient) Put(nodeIndex int, key, value string) error {
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
func (c *ChaosClient) Get(nodeIndex int, key string) (string, error) {
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

// GetClusterStatus retrieves the status of the cluster
func (c *ChaosClient) GetClusterStatus(nodeIndex int) (map[string]interface{}, error) {
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
func (c *ChaosClient) WaitForClusterStability(timeout time.Duration) error {
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

// BlockNetworkBetweenNodes blocks network traffic between two nodes
func (c *ChaosClient) BlockNetworkBetweenNodes(sourceNode, targetNode string) error {
	cmd := exec.Command("iptables", "-A", "FORWARD", "-s", sourceNode, "-d", targetNode, "-j", "DROP")
	return cmd.Run()
}

// UnblockNetworkBetweenNodes unblocks network traffic between two nodes
func (c *ChaosClient) UnblockNetworkBetweenNodes(sourceNode, targetNode string) error {
	cmd := exec.Command("iptables", "-D", "FORWARD", "-s", sourceNode, "-d", targetNode, "-j", "DROP")
	return cmd.Run()
}

// StopNode stops a node
func (c *ChaosClient) StopNode(nodeName string) error {
	cmd := exec.Command("docker", "stop", nodeName)
	return cmd.Run()
}

// StartNode starts a node
func (c *ChaosClient) StartNode(nodeName string) error {
	cmd := exec.Command("docker", "start", nodeName)
	return cmd.Run()
}

// InjectLatency injects network latency between nodes
func (c *ChaosClient) InjectLatency(nodeName string, latencyMs int) error {
	cmd := exec.Command("docker", "exec", nodeName, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", fmt.Sprintf("%dms", latencyMs))
	return cmd.Run()
}

// RemoveLatency removes network latency between nodes
func (c *ChaosClient) RemoveLatency(nodeName string) error {
	cmd := exec.Command("docker", "exec", nodeName, "tc", "qdisc", "del", "dev", "eth0", "root")
	return cmd.Run()
}

// TestNetworkPartition tests the system's behavior during a network partition
func TestNetworkPartition(t *testing.T) {
	client := NewChaosClient()

	// Wait for cluster to stabilize
	err := client.WaitForClusterStability(60 * time.Second)
	require.NoError(t, err, "Cluster failed to stabilize")

	// Write data to node 0
	key := "partition-test-key"
	value := "partition-test-value"

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

	// Create a network partition between nodes 0,1 and nodes 2,3,4
	// This simulates a split-brain scenario
	nodeParts := [][]string{
		{"node1", "node2"},
		{"node3", "node4", "node5"},
	}

	// Block traffic between the two partitions
	for _, sourceNode := range nodeParts[0] {
		for _, targetNode := range nodeParts[1] {
			t.Logf("Blocking traffic from %s to %s", sourceNode, targetNode)
			err := client.BlockNetworkBetweenNodes(sourceNode, targetNode)
			require.NoError(t, err, "Failed to block network between %s and %s", sourceNode, targetNode)

			err = client.BlockNetworkBetweenNodes(targetNode, sourceNode)
			require.NoError(t, err, "Failed to block network between %s and %s", targetNode, sourceNode)
		}
	}

	// Allow time for the network partition to be detected
	time.Sleep(10 * time.Second)

	// Write a new key-value pair to the first partition
	partitionKey1 := "partition1-key"
	partitionValue1 := "partition1-value"

	err = client.Put(0, partitionKey1, partitionValue1)
	require.NoError(t, err, "Failed to put key-value pair to first partition")

	// Write a different key-value pair to the second partition
	partitionKey2 := "partition2-key"
	partitionValue2 := "partition2-value"

	err = client.Put(2, partitionKey2, partitionValue2)
	require.NoError(t, err, "Failed to put key-value pair to second partition")

	// Allow time for replication within partitions
	time.Sleep(5 * time.Second)

	// Verify data written to the first partition is available within that partition
	for i := 0; i < 2; i++ {
		readValue, err := client.Get(i, partitionKey1)
		require.NoError(t, err, "Failed to get value from node %d in first partition", i)
		assert.Equal(t, partitionValue1, readValue, "Value read from node %d in first partition doesn't match expected value", i)
	}

	// Verify data written to the second partition is available within that partition
	for i := 2; i < 5; i++ {
		readValue, err := client.Get(i, partitionKey2)
		require.NoError(t, err, "Failed to get value from node %d in second partition", i)
		assert.Equal(t, partitionValue2, readValue, "Value read from node %d in second partition doesn't match expected value", i)
	}

	// Heal the network partition
	for _, sourceNode := range nodeParts[0] {
		for _, targetNode := range nodeParts[1] {
			t.Logf("Unblocking traffic from %s to %s", sourceNode, targetNode)
			err := client.UnblockNetworkBetweenNodes(sourceNode, targetNode)
			require.NoError(t, err, "Failed to unblock network between %s and %s", sourceNode, targetNode)

			err = client.UnblockNetworkBetweenNodes(targetNode, sourceNode)
			require.NoError(t, err, "Failed to unblock network between %s and %s", targetNode, sourceNode)
		}
	}

	// Allow time for the cluster to heal and reconcile
	time.Sleep(30 * time.Second)

	// Wait for cluster to stabilize
	err = client.WaitForClusterStability(60 * time.Second)
	require.NoError(t, err, "Cluster failed to stabilize after healing network partition")

	// Verify data from both partitions is now available from all nodes
	// Note: In case of conflicts, the system should have a conflict resolution strategy
	for i := 0; i < len(client.nodes); i++ {
		// Check the first partition's key
		readValue, err := client.Get(i, partitionKey1)
		require.NoError(t, err, "Failed to get partition1 key from node %d after healing", i)
		assert.Equal(t, partitionValue1, readValue, "Partition1 value read from node %d doesn't match expected value after healing", i)

		// Check the second partition's key
		readValue, err = client.Get(i, partitionKey2)
		require.NoError(t, err, "Failed to get partition2 key from node %d after healing", i)
		assert.Equal(t, partitionValue2, readValue, "Partition2 value read from node %d doesn't match expected value after healing", i)
	}
}

// TestNodeFailureAndRecovery tests the system's behavior when nodes fail and recover
func TestNodeFailureAndRecovery(t *testing.T) {
	client := NewChaosClient()

	// Wait for cluster to stabilize
	err := client.WaitForClusterStability(60 * time.Second)
	require.NoError(t, err, "Cluster failed to stabilize")

	// Write data to all nodes
	for i := 0; i < len(client.nodes); i++ {
		key := fmt.Sprintf("failure-test-key-%d", i)
		value := fmt.Sprintf("failure-test-value-%d", i)

		err := client.Put(i, key, value)
		require.NoError(t, err, "Failed to put key-value pair to node %d", i)
	}

	// Allow time for replication
	time.Sleep(5 * time.Second)

	// Stop node 2
	t.Log("Stopping node3")
	err = client.StopNode("node3")
	require.NoError(t, err, "Failed to stop node3")

	// Allow time for failure detection
	time.Sleep(10 * time.Second)

	// Write new data to a different node
	newKey := "after-failure-key"
	newValue := "after-failure-value"

	err = client.Put(0, newKey, newValue)
	require.NoError(t, err, "Failed to put key-value pair after node failure")

	// Allow time for replication
	time.Sleep(5 * time.Second)

	// Verify data is available from all running nodes
	for i := 0; i < len(client.nodes); i++ {
		if i == 2 { // Skip the failed node
			continue
		}

		readValue, err := client.Get(i, newKey)
		require.NoError(t, err, "Failed to get value from node %d after node failure", i)
		assert.Equal(t, newValue, readValue, "Value read from node %d doesn't match expected value after node failure", i)
	}

	// Restart the failed node
	t.Log("Starting node3")
	err = client.StartNode("node3")
	require.NoError(t, err, "Failed to start node3")

	// Allow time for the node to recover and catch up
	time.Sleep(30 * time.Second)

	// Wait for cluster to stabilize
	err = client.WaitForClusterStability(60 * time.Second)
	require.NoError(t, err, "Cluster failed to stabilize after node recovery")

	// Verify the recovered node has the data written while it was down
	readValue, err := client.Get(2, newKey)
	require.NoError(t, err, "Failed to get value from recovered node")
	assert.Equal(t, newValue, readValue, "Value read from recovered node doesn't match expected value")
}

// TestRandomNodeTermination tests the system's behavior with random node terminations
func TestRandomNodeTermination(t *testing.T) {
	client := NewChaosClient()

	// Wait for cluster to stabilize
	err := client.WaitForClusterStability(60 * time.Second)
	require.NoError(t, err, "Cluster failed to stabilize")

	// Write initial data
	initialKey := "initial-key"
	initialValue := "initial-value"

	err = client.Put(0, initialKey, initialValue)
	require.NoError(t, err, "Failed to put initial key-value pair")

	// Allow time for replication
	time.Sleep(5 * time.Second)

	// Randomly terminate and restart nodes multiple times
	for i := 0; i < 3; i++ {
		// Stop a random node (using a deterministic pattern for testing)
		nodeIndex := (i % 3) + 1 // Use nodes 1, 2, 3 (indexes 0, 1, 2)
		nodeName := fmt.Sprintf("node%d", nodeIndex+1)

		t.Logf("Iteration %d: Stopping %s", i, nodeName)
		err = client.StopNode(nodeName)
		require.NoError(t, err, "Failed to stop %s", nodeName)

		// Allow time for failure detection
		time.Sleep(10 * time.Second)

		// Write new data
		key := fmt.Sprintf("chaos-key-%d", i)
		value := fmt.Sprintf("chaos-value-%d", i)

		// Write to a node that's still running
		writeNodeIndex := (nodeIndex + 2) % 5
		err = client.Put(writeNodeIndex, key, value)
		require.NoError(t, err, "Failed to put key-value pair to node %d during chaos", writeNodeIndex)

		// Allow time for replication
		time.Sleep(5 * time.Second)

		// Restart the node
		t.Logf("Iteration %d: Starting %s", i, nodeName)
		err = client.StartNode(nodeName)
		require.NoError(t, err, "Failed to start %s", nodeName)

		// Allow time for the node to recover
		time.Sleep(20 * time.Second)
	}

	// Allow time for the cluster to fully recover
	time.Sleep(30 * time.Second)

	// Wait for cluster to stabilize
	err = client.WaitForClusterStability(60 * time.Second)
	require.NoError(t, err, "Cluster failed to stabilize after chaos testing")

	// Verify all data is available from all nodes
	// First, check the initial data
	for i := 0; i < len(client.nodes); i++ {
		readValue, err := client.Get(i, initialKey)
		require.NoError(t, err, "Failed to get initial value from node %d after chaos", i)
		assert.Equal(t, initialValue, readValue, "Initial value read from node %d doesn't match expected value after chaos", i)
	}

	// Then, check all the data written during chaos
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("chaos-key-%d", i)
		value := fmt.Sprintf("chaos-value-%d", i)

		for j := 0; j < len(client.nodes); j++ {
			readValue, err := client.Get(j, key)
			require.NoError(t, err, "Failed to get chaos value %d from node %d after chaos", i, j)
			assert.Equal(t, value, readValue, "Chaos value %d read from node %d doesn't match expected value after chaos", i, j)
		}
	}
}

// TestNetworkLatency tests the system's behavior with network latency
func TestNetworkLatency(t *testing.T) {
	client := NewChaosClient()

	// Wait for cluster to stabilize
	err := client.WaitForClusterStability(60 * time.Second)
	require.NoError(t, err, "Cluster failed to stabilize")

	// Inject latency to node 1
	t.Log("Injecting 200ms latency to node1")
	err = client.InjectLatency("node1", 200)
	require.NoError(t, err, "Failed to inject latency to node1")

	// Allow time for the latency to take effect
	time.Sleep(5 * time.Second)

	// Write data to the node with latency
	latencyKey := "latency-test-key"
	latencyValue := "latency-test-value"

	err = client.Put(0, latencyKey, latencyValue)
	require.NoError(t, err, "Failed to put key-value pair to node with latency")

	// Allow extra time for replication due to latency
	time.Sleep(10 * time.Second)

	// Verify data is available from all nodes
	for i := 0; i < len(client.nodes); i++ {
		readValue, err := client.Get(i, latencyKey)
		require.NoError(t, err, "Failed to get value from node %d with latency in the system", i)
		assert.Equal(t, latencyValue, readValue, "Value read from node %d doesn't match expected value with latency in the system", i)
	}

	// Remove the latency
	t.Log("Removing latency from node1")
	err = client.RemoveLatency("node1")
	require.NoError(t, err, "Failed to remove latency from node1")

	// Allow time for the system to stabilize
	time.Sleep(10 * time.Second)

	// Write another key-value pair
	postLatencyKey := "post-latency-key"
	postLatencyValue := "post-latency-value"

	err = client.Put(0, postLatencyKey, postLatencyValue)
	require.NoError(t, err, "Failed to put key-value pair after removing latency")

	// Allow time for replication
	time.Sleep(5 * time.Second)

	// Verify data is available from all nodes
	for i := 0; i < len(client.nodes); i++ {
		readValue, err := client.Get(i, postLatencyKey)
		require.NoError(t, err, "Failed to get value from node %d after removing latency", i)
		assert.Equal(t, postLatencyValue, readValue, "Value read from node %d doesn't match expected value after removing latency", i)
	}
}
