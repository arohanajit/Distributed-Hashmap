package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

// SWIM Protocol Constants
const (
	SwimProtocolPeriod = 1 * time.Second        // Protocol period
	SwimIndirectNodes  = 3                      // Number of nodes for indirect pings
	SwimPingTimeout    = 500 * time.Millisecond // Ping timeout
	SwimMaxBroadcasts  = 3                      // Number of broadcasts per message
	SwimSuspectTimeout = 5 * time.Second        // Time before suspect node is marked dead
	SwimNodeDeleteTime = 24 * time.Hour         // Time before dead node is removed from list
)

// NodeState represents the state of a node in the SWIM protocol
type NodeState int

const (
	NodeStateAlive NodeState = iota
	NodeStateSuspect
	NodeStateDead
)

// SwimNode represents a node in the SWIM protocol
type SwimNode struct {
	Node
	State       NodeState
	StateTime   time.Time
	Incarnation uint64 // Incarnation number (for conflict resolution)
}

// Ping represents a ping message
type Ping struct {
	SourceID string
	TargetID string
	Sequence uint64
}

// PingReq represents an indirect ping request
type PingReq struct {
	SourceID string
	TargetID string
	RelayID  string
	Sequence uint64
}

// Ack represents an acknowledgment message
type Ack struct {
	SourceID  string
	TargetID  string
	Sequence  uint64
	Timestamp time.Time
}

// SwimUpdate represents an update in node status
type SwimUpdate struct {
	Node        Node
	State       NodeState
	Incarnation uint64
	Time        time.Time
}

// SwimGossip represents the SWIM gossip protocol
type SwimGossip struct {
	mu             sync.RWMutex
	self           SwimNode
	nodes          map[string]*SwimNode
	sequenceNumber uint64
	updates        []SwimUpdate // Recent updates for gossip
	udpListener    *net.UDPConn // UDP listener for SWIM protocol
	tcpListener    net.Listener // TCP listener for full state sync
	clusterManager *ClusterManagerImpl
	isRunning      bool
	stopChan       chan struct{}
	updateChan     chan SwimUpdate
	config         SwimConfig
}

// SwimConfig contains configuration for the SWIM protocol
type SwimConfig struct {
	UDPPort        int           // UDP port for SWIM messages
	TCPPort        int           // TCP port for full state sync
	SuspectTimeout time.Duration // Timeout before suspect -> dead
	ProtocolPeriod time.Duration // Protocol period
	IndirectNodes  int           // Number of nodes for indirect pings
	MaxBroadcasts  int           // Number of broadcasts per update
}

// NewSwimGossip creates a new instance of SwimGossip
func NewSwimGossip(self Node) (*SwimGossip, error) {
	swimSelf := SwimNode{
		Node:        self,
		State:       NodeStateAlive,
		StateTime:   time.Now(),
		Incarnation: 1,
	}

	sg := &SwimGossip{
		self:       swimSelf,
		nodes:      make(map[string]*SwimNode),
		updates:    make([]SwimUpdate, 0),
		stopChan:   make(chan struct{}),
		updateChan: make(chan SwimUpdate, 100),
		config: SwimConfig{
			UDPPort:        7946,
			TCPPort:        7947,
			SuspectTimeout: SwimSuspectTimeout,
			ProtocolPeriod: SwimProtocolPeriod,
			IndirectNodes:  SwimIndirectNodes,
			MaxBroadcasts:  SwimMaxBroadcasts,
		},
	}

	return sg, nil
}

// SetConfig sets the configuration for the SWIM protocol
func (sg *SwimGossip) SetConfig(config SwimConfig) {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	// Only update if the protocol isn't running
	if !sg.isRunning {
		if config.UDPPort > 0 {
			sg.config.UDPPort = config.UDPPort
		}
		if config.TCPPort > 0 {
			sg.config.TCPPort = config.TCPPort
		}
		if config.SuspectTimeout > 0 {
			sg.config.SuspectTimeout = config.SuspectTimeout
		}
		if config.ProtocolPeriod > 0 {
			sg.config.ProtocolPeriod = config.ProtocolPeriod
		}
		if config.IndirectNodes > 0 {
			sg.config.IndirectNodes = config.IndirectNodes
		}
		if config.MaxBroadcasts > 0 {
			sg.config.MaxBroadcasts = config.MaxBroadcasts
		}
	}
}

// Start starts the SWIM protocol
func (sg *SwimGossip) Start(ctx context.Context) error {
	sg.mu.Lock()
	if sg.isRunning {
		sg.mu.Unlock()
		return nil
	}
	sg.isRunning = true
	sg.mu.Unlock()

	// Start UDP listener
	addr, err := getAddressFromNode(sg.self.Node)
	if err != nil {
		return fmt.Errorf("failed to get address: %v", err)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", addr.IP.String(), sg.config.UDPPort))
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %v", err)
	}

	sg.udpListener, err = net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to start UDP listener: %v", err)
	}

	// Start TCP listener for state sync
	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", addr.IP.String(), sg.config.TCPPort))
	if err != nil {
		return fmt.Errorf("failed to resolve TCP address: %v", err)
	}

	sg.tcpListener, err = net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return fmt.Errorf("failed to start TCP listener: %v", err)
	}

	// Start protocol goroutines
	go sg.listenUDP()
	go sg.listenTCP()
	go sg.protocol(ctx)
	go sg.updateProcessor(ctx)

	return nil
}

// Stop stops the SWIM protocol
func (sg *SwimGossip) Stop() {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	if !sg.isRunning {
		return
	}

	close(sg.stopChan)
	sg.isRunning = false

	if sg.udpListener != nil {
		sg.udpListener.Close()
	}

	if sg.tcpListener != nil {
		sg.tcpListener.Close()
	}
}

// AddNode adds a node to the SWIM protocol
func (sg *SwimGossip) AddNode(nodeID, address string) {
	node := Node{
		ID:      nodeID,
		Address: address,
		Status:  NodeStatusActive,
	}

	swimNode := &SwimNode{
		Node:        node,
		State:       NodeStateAlive,
		StateTime:   time.Now(),
		Incarnation: 0,
	}

	sg.mu.Lock()
	defer sg.mu.Unlock()

	// Check if node already exists
	if n, exists := sg.nodes[nodeID]; exists {
		// Only update if new incarnation is higher
		if swimNode.Incarnation > n.Incarnation {
			sg.nodes[nodeID] = swimNode
			sg.addUpdate(SwimUpdate{
				Node:        node,
				State:       NodeStateAlive,
				Incarnation: swimNode.Incarnation,
				Time:        time.Now(),
			})
		}
		return
	}

	sg.nodes[nodeID] = swimNode
	sg.addUpdate(SwimUpdate{
		Node:        node,
		State:       NodeStateAlive,
		Incarnation: swimNode.Incarnation,
		Time:        time.Now(),
	})
}

// RemoveNode removes a node from the SWIM protocol
func (sg *SwimGossip) RemoveNode(nodeID string) {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	if node, exists := sg.nodes[nodeID]; exists {
		node.State = NodeStateDead
		node.StateTime = time.Now()

		sg.addUpdate(SwimUpdate{
			Node:        node.Node,
			State:       NodeStateDead,
			Incarnation: node.Incarnation,
			Time:        time.Now(),
		})
	}
}

// ListNodes returns a list of all nodes
func (sg *SwimGossip) ListNodes() []Node {
	sg.mu.RLock()
	defer sg.mu.RUnlock()

	nodes := make([]Node, 0, len(sg.nodes))
	for _, node := range sg.nodes {
		if node.State != NodeStateDead {
			nodes = append(nodes, node.Node)
		}
	}
	return nodes
}

// protocol runs the SWIM protocol
func (sg *SwimGossip) protocol(ctx context.Context) {
	ticker := time.NewTicker(sg.config.ProtocolPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sg.stopChan:
			return
		case <-ticker.C:
			sg.protocolPeriod()
		}
	}
}

// protocolPeriod performs one round of the SWIM protocol
func (sg *SwimGossip) protocolPeriod() {
	// Select a random node to ping
	target := sg.selectRandomNode()
	if target == nil {
		return // No nodes to ping
	}

	// Attempt direct ping
	ok := sg.ping(target.Node)
	if ok {
		return // Ping successful
	}

	// If direct ping fails, try indirect pings
	indirectNodes := sg.selectIndirectNodes(target.ID)
	for _, relay := range indirectNodes {
		sg.pingIndirect(target.Node, relay.Node)
	}
}

// ping sends a direct ping to a node
func (sg *SwimGossip) ping(node Node) bool {
	sg.mu.Lock()
	seq := sg.sequenceNumber
	sg.sequenceNumber++
	sg.mu.Unlock()

	ping := Ping{
		SourceID: sg.self.ID,
		TargetID: node.ID,
		Sequence: seq,
	}

	// Get UDP address
	addr, err := getNodeAddr(node, sg.config.UDPPort)
	if err != nil {
		return false
	}

	// Marshal ping message
	data, err := json.Marshal(ping)
	if err != nil {
		return false
	}

	// Create message with header
	msg := append([]byte{1}, data...) // 1 = ping

	// Send ping
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return false
	}
	defer conn.Close()

	_, err = conn.Write(msg)
	if err != nil {
		return false
	}

	// Wait for ack
	conn.SetReadDeadline(time.Now().Add(SwimPingTimeout))
	respBuf := make([]byte, 1024)
	n, _, err := conn.ReadFromUDP(respBuf)
	if err != nil {
		// Mark node as suspect if no ack received
		sg.markSuspect(node.ID)
		return false
	}

	// Check if response is an ack
	if n < 1 || respBuf[0] != 3 { // 3 = ack
		sg.markSuspect(node.ID)
		return false
	}

	var ack Ack
	if err := json.Unmarshal(respBuf[1:n], &ack); err != nil {
		sg.markSuspect(node.ID)
		return false
	}

	if ack.TargetID != sg.self.ID || ack.SourceID != node.ID || ack.Sequence != seq {
		sg.markSuspect(node.ID)
		return false
	}

	// Update node timestamp
	sg.markAlive(node.ID)
	return true
}

// pingIndirect sends an indirect ping to a node through a relay
func (sg *SwimGossip) pingIndirect(target Node, relay Node) bool {
	sg.mu.Lock()
	seq := sg.sequenceNumber
	sg.sequenceNumber++
	sg.mu.Unlock()

	pingReq := PingReq{
		SourceID: sg.self.ID,
		TargetID: target.ID,
		RelayID:  relay.ID,
		Sequence: seq,
	}

	// Get UDP address of relay
	addr, err := getNodeAddr(relay, sg.config.UDPPort)
	if err != nil {
		return false
	}

	// Marshal ping request
	data, err := json.Marshal(pingReq)
	if err != nil {
		return false
	}

	// Create message with header
	msg := append([]byte{2}, data...) // 2 = ping req

	// Send ping request
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return false
	}
	defer conn.Close()

	_, err = conn.Write(msg)
	if err != nil {
		return false
	}

	return true // We don't wait for a response to indirect pings
}

// listenUDP listens for UDP messages
func (sg *SwimGossip) listenUDP() {
	buf := make([]byte, 2048)

	for {
		n, addr, err := sg.udpListener.ReadFromUDP(buf)
		if err != nil {
			// Check if we're shutting down
			select {
			case <-sg.stopChan:
				return
			default:
				// Continue if temporary error
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					continue
				}
				return
			}
		}

		if n < 1 {
			continue
		}

		// Handle message based on type
		switch buf[0] {
		case 1: // Ping
			sg.handlePing(buf[1:n], addr)
		case 2: // PingReq
			sg.handlePingReq(buf[1:n], addr)
		case 3: // Ack (already handled in ping())
		case 4: // Update
			sg.handleUpdate(buf[1:n])
		}
	}
}

// handlePing handles a ping message
func (sg *SwimGossip) handlePing(data []byte, addr *net.UDPAddr) {
	var ping Ping
	if err := json.Unmarshal(data, &ping); err != nil {
		return
	}

	// If ping is not for us, drop it
	if ping.TargetID != sg.self.ID {
		return
	}

	// Send ack
	ack := Ack{
		SourceID:  sg.self.ID,
		TargetID:  ping.SourceID,
		Sequence:  ping.Sequence,
		Timestamp: time.Now(),
	}

	ackData, err := json.Marshal(ack)
	if err != nil {
		return
	}

	// Add type byte
	msg := append([]byte{3}, ackData...) // 3 = ack

	// Send ack
	sg.udpListener.WriteToUDP(msg, addr)

	// Include some gossip with the ack
	sg.sendUpdates(addr)
}

// handlePingReq handles a ping request message
func (sg *SwimGossip) handlePingReq(data []byte, addr *net.UDPAddr) {
	var pingReq PingReq
	if err := json.Unmarshal(data, &pingReq); err != nil {
		return
	}

	// If ping request is not for us, drop it
	if pingReq.RelayID != sg.self.ID {
		return
	}

	// Check if target node exists
	sg.mu.RLock()
	target, exists := sg.nodes[pingReq.TargetID]
	sg.mu.RUnlock()

	if !exists {
		return
	}

	// Forward ping to target
	ok := sg.ping(target.Node)

	// If successful, also send success to source
	if ok {
		sg.mu.RLock()
		source, exists := sg.nodes[pingReq.SourceID]
		sg.mu.RUnlock()

		if !exists {
			return
		}

		sourceAddr, err := getNodeAddr(source.Node, sg.config.UDPPort)
		if err != nil {
			return
		}

		// Create an update message
		update := SwimUpdate{
			Node:        target.Node,
			State:       NodeStateAlive,
			Incarnation: target.Incarnation,
			Time:        time.Now(),
		}

		updateData, err := json.Marshal(update)
		if err != nil {
			return
		}

		// Add type byte
		msg := append([]byte{4}, updateData...) // 4 = update

		// Send update
		conn, err := net.DialUDP("udp", nil, sourceAddr)
		if err != nil {
			return
		}
		defer conn.Close()

		conn.Write(msg)
	}
}

// handleUpdate handles an update message
func (sg *SwimGossip) handleUpdate(data []byte) {
	var update SwimUpdate
	if err := json.Unmarshal(data, &update); err != nil {
		return
	}

	sg.updateChan <- update
}

// updateProcessor processes updates from the update channel
func (sg *SwimGossip) updateProcessor(ctx context.Context) {
	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sg.stopChan:
			return
		case <-cleanupTicker.C:
			sg.cleanupDeadNodes()
		case update := <-sg.updateChan:
			sg.processUpdate(update)
		}
	}
}

// processUpdate processes an update
func (sg *SwimGossip) processUpdate(update SwimUpdate) {
	// Skip updates about ourselves
	if update.Node.ID == sg.self.ID {
		return
	}

	sg.mu.Lock()
	defer sg.mu.Unlock()

	node, exists := sg.nodes[update.Node.ID]

	// Handle based on node state
	if !exists {
		// New node
		newNode := &SwimNode{
			Node:        update.Node,
			State:       update.State,
			StateTime:   update.Time,
			Incarnation: update.Incarnation,
		}
		sg.nodes[update.Node.ID] = newNode
		sg.addUpdate(update)

		// Notify cluster manager
		if sg.clusterManager != nil && update.State == NodeStateAlive {
			go func(n Node) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				sg.clusterManager.JoinNode(ctx, n)
			}(update.Node)
		}
		return
	}

	// Existing node - apply update if newer incarnation or same incarnation but worse state
	if update.Incarnation > node.Incarnation ||
		(update.Incarnation == node.Incarnation && update.State > node.State) {

		oldState := node.State
		node.Incarnation = update.Incarnation
		node.State = update.State
		node.StateTime = update.Time

		sg.addUpdate(update)

		// Notify cluster manager if state changed
		if sg.clusterManager != nil && oldState != node.State {
			if node.State == NodeStateAlive {
				go func(n Node) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					sg.clusterManager.JoinNode(ctx, n)
				}(node.Node)
			} else if node.State == NodeStateDead {
				go func(id string) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					sg.clusterManager.LeaveNode(ctx, id)
				}(node.ID)
			}
		}
	}
}

// markSuspect marks a node as suspect
func (sg *SwimGossip) markSuspect(nodeID string) {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	node, exists := sg.nodes[nodeID]
	if !exists || node.State == NodeStateDead {
		return
	}

	if node.State == NodeStateAlive {
		node.State = NodeStateSuspect
		node.StateTime = time.Now()

		sg.addUpdate(SwimUpdate{
			Node:        node.Node,
			State:       NodeStateSuspect,
			Incarnation: node.Incarnation,
			Time:        time.Now(),
		})
	} else if node.State == NodeStateSuspect {
		// If node has been suspect for too long, mark as dead
		if time.Since(node.StateTime) > sg.config.SuspectTimeout {
			node.State = NodeStateDead
			node.StateTime = time.Now()

			sg.addUpdate(SwimUpdate{
				Node:        node.Node,
				State:       NodeStateDead,
				Incarnation: node.Incarnation,
				Time:        time.Now(),
			})

			// Notify cluster manager
			if sg.clusterManager != nil {
				go func(id string) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					sg.clusterManager.LeaveNode(ctx, id)
				}(node.ID)
			}
		}
	}
}

// markAlive marks a node as alive
func (sg *SwimGossip) markAlive(nodeID string) {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	node, exists := sg.nodes[nodeID]
	if !exists {
		return
	}

	// If node is suspect or dead, we need to refute with a higher incarnation
	if node.State != NodeStateAlive {
		node.Incarnation++
		node.State = NodeStateAlive
		node.StateTime = time.Now()

		sg.addUpdate(SwimUpdate{
			Node:        node.Node,
			State:       NodeStateAlive,
			Incarnation: node.Incarnation,
			Time:        time.Now(),
		})

		// Notify cluster manager
		if sg.clusterManager != nil {
			go func(n Node) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				sg.clusterManager.JoinNode(ctx, n)
			}(node.Node)
		}
	}
}

// cleanupDeadNodes removes nodes that have been dead for too long
func (sg *SwimGossip) cleanupDeadNodes() {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	for id, node := range sg.nodes {
		if node.State == NodeStateDead && time.Since(node.StateTime) > SwimNodeDeleteTime {
			delete(sg.nodes, id)
		}
	}
}

// sendUpdates sends recent updates to a node
func (sg *SwimGossip) sendUpdates(addr *net.UDPAddr) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()

	if len(sg.updates) == 0 {
		return
	}

	// Select random updates to send (up to MaxBroadcasts)
	broadcasts := sg.config.MaxBroadcasts
	if broadcasts > len(sg.updates) {
		broadcasts = len(sg.updates)
	}

	indexes := rand.Perm(len(sg.updates))[:broadcasts]
	for _, idx := range indexes {
		update := sg.updates[idx]

		// Marshal update
		updateData, err := json.Marshal(update)
		if err != nil {
			continue
		}

		// Add type byte
		msg := append([]byte{4}, updateData...) // 4 = update

		// Send update
		sg.udpListener.WriteToUDP(msg, addr)
	}
}

// addUpdate adds an update to the updates list
func (sg *SwimGossip) addUpdate(update SwimUpdate) {
	// Keep updates list from growing too large (circular buffer)
	maxUpdates := 100
	if len(sg.updates) >= maxUpdates {
		sg.updates = sg.updates[1:]
	}
	sg.updates = append(sg.updates, update)
}

// listenTCP listens for TCP connections for full state sync
func (sg *SwimGossip) listenTCP() {
	for {
		conn, err := sg.tcpListener.Accept()
		if err != nil {
			// Check if we're shutting down
			select {
			case <-sg.stopChan:
				return
			default:
				// Continue if temporary error
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					continue
				}
				return
			}
		}

		go sg.handleTCP(conn)
	}
}

// handleTCP handles a TCP connection for full state sync
func (sg *SwimGossip) handleTCP(conn net.Conn) {
	defer conn.Close()

	// Read request
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	// Check if it's a state sync request
	if n != 1 || buf[0] != 5 { // 5 = state sync request
		return
	}

	// Get node list
	sg.mu.RLock()
	nodes := make([]*SwimNode, 0, len(sg.nodes))
	for _, node := range sg.nodes {
		nodes = append(nodes, node)
	}
	sg.mu.RUnlock()

	// Send node list
	data, err := json.Marshal(nodes)
	if err != nil {
		return
	}

	conn.Write(data)
}

// RequestSync requests a full state sync from a node
func (sg *SwimGossip) RequestSync(node Node) error {
	// Get TCP address
	addr, err := getNodeAddr(node, sg.config.TCPPort)
	if err != nil {
		return err
	}

	// Connect to node
	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send sync request
	_, err = conn.Write([]byte{5}) // 5 = state sync request
	if err != nil {
		return err
	}

	// Read response
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(conn); err != nil {
		return err
	}

	// Parse response
	var nodes []*SwimNode
	if err := json.Unmarshal(buf.Bytes(), &nodes); err != nil {
		return err
	}

	// Update local state
	sg.mu.Lock()
	defer sg.mu.Unlock()

	for _, node := range nodes {
		existingNode, exists := sg.nodes[node.ID]

		// Skip our own node
		if node.ID == sg.self.ID {
			continue
		}

		if !exists || node.Incarnation > existingNode.Incarnation {
			sg.nodes[node.ID] = node
			sg.addUpdate(SwimUpdate{
				Node:        node.Node,
				State:       node.State,
				Incarnation: node.Incarnation,
				Time:        time.Now(),
			})

			// Notify cluster manager if node is alive
			if sg.clusterManager != nil && node.State == NodeStateAlive {
				go func(n Node) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					sg.clusterManager.JoinNode(ctx, n)
				}(node.Node)
			}
		}
	}

	return nil
}

// selectRandomNode selects a random node from the node list
func (sg *SwimGossip) selectRandomNode() *SwimNode {
	sg.mu.RLock()
	defer sg.mu.RUnlock()

	// Get alive nodes
	aliveNodes := make([]*SwimNode, 0, len(sg.nodes))
	for _, node := range sg.nodes {
		if node.ID != sg.self.ID && node.State != NodeStateDead {
			aliveNodes = append(aliveNodes, node)
		}
	}

	if len(aliveNodes) == 0 {
		return nil
	}

	// Select a random node
	return aliveNodes[rand.Intn(len(aliveNodes))]
}

// selectIndirectNodes selects random nodes for indirect pings
func (sg *SwimGossip) selectIndirectNodes(excludeID string) []*SwimNode {
	sg.mu.RLock()
	defer sg.mu.RUnlock()

	// Get alive nodes excluding the target node
	aliveNodes := make([]*SwimNode, 0, len(sg.nodes))
	for _, node := range sg.nodes {
		if node.ID != sg.self.ID && node.ID != excludeID && node.State == NodeStateAlive {
			aliveNodes = append(aliveNodes, node)
		}
	}

	// If not enough nodes, return all of them
	if len(aliveNodes) <= sg.config.IndirectNodes {
		return aliveNodes
	}

	// Select random nodes
	selected := make([]*SwimNode, sg.config.IndirectNodes)
	indexes := rand.Perm(len(aliveNodes))[:sg.config.IndirectNodes]
	for i, idx := range indexes {
		selected[i] = aliveNodes[idx]
	}

	return selected
}

// SetClusterManager sets the cluster manager for callbacks
func (sg *SwimGossip) SetClusterManager(cm *ClusterManagerImpl) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.clusterManager = cm
}

// getNodeAddr gets the UDP address of a node
func getNodeAddr(node Node, port int) (*net.UDPAddr, error) {
	addr, err := getAddressFromNode(node)
	if err != nil {
		return nil, err
	}

	return net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", addr.IP.String(), port))
}

// getAddressFromNode extracts host/port from node address
func getAddressFromNode(node Node) (*net.TCPAddr, error) {
	return net.ResolveTCPAddr("tcp", node.Address)
}
