package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/arohanajit/Distributed-Hashmap/internal/api/rest"
	"github.com/arohanajit/Distributed-Hashmap/internal/cluster"
	"github.com/arohanajit/Distributed-Hashmap/internal/config"
	"github.com/arohanajit/Distributed-Hashmap/internal/storage"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	defaultPort    = 8080
	defaultTimeout = 10 * time.Second
)

func main() {
	// Load configuration
	config := config.LoadConfig()
	if err := config.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	// Initialize storage
	store := storage.NewStore()

	// Initialize shard manager with test implementation
	shardMgr := &testShardManager{
		responsibleNodes: make(map[string][]string),
		nodeAddresses:    make(map[string]string),
		currentNodeID:    config.NodeID,
	}

	// Initialize failure detector
	failureDetector := cluster.NewFailureDetector(config.HeartbeatInterval, 3)

	// Initialize gossip protocol
	gossipProtocol := cluster.NewGossipProtocol(config.NodeID, failureDetector)

	// Initialize health checker
	healthChecker := cluster.NewHTTPHealthChecker(nil)

	// Initialize cluster manager
	clusterManager := cluster.NewClusterManager(shardMgr, healthChecker, failureDetector, gossipProtocol)

	// Initialize discovery manager
	discoveryMgr := cluster.NewDiscoveryManager(cluster.Node{
		ID:      config.NodeID,
		Address: fmt.Sprintf("%s:%d", config.Host, config.Port),
		Status:  cluster.NodeStatusActive,
	})

	// Load initial nodes from environment
	if err := discoveryMgr.LoadNodesFromEnv(); err != nil {
		logger.Error("Failed to load nodes from environment", zap.Error(err))
	}

	// Initialize API handlers
	router := mux.NewRouter()

	// Register key-value operation handlers
	keyHandler := rest.NewKeyHandler(store)
	router.HandleFunc("/keys/{key}", keyHandler.Put).Methods(http.MethodPut)
	router.HandleFunc("/keys/{key}", keyHandler.Get).Methods(http.MethodGet)
	router.HandleFunc("/keys/{key}", keyHandler.Delete).Methods(http.MethodDelete)

	// Register cluster management handlers
	clusterHandler := rest.NewClusterHandler(clusterManager)
	clusterHandler.RegisterRoutes(router)

	// Add middleware
	router.Use(rest.LoggingMiddleware)
	router.Use(rest.TimeoutMiddleware(10 * time.Second))
	router.Use(rest.NewShardLookupMiddleware(shardMgr, config.NodeID).Middleware)

	// Start server
	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	logger.Info("Starting server", zap.String("address", addr))

	server := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		logger.Fatal("Failed to start server", zap.Error(err))
	}
}

// testShardManager implements the ShardManager interface for testing
type testShardManager struct {
	responsibleNodes map[string][]string
	nodeAddresses    map[string]string
	currentNodeID    string
}

func (m *testShardManager) GetResponsibleNodes(key string) []string {
	return m.responsibleNodes[key]
}

func (m *testShardManager) GetSuccessorNodes(nodeID string, count int) []string {
	return nil
}

func (m *testShardManager) GetNodeAddress(nodeID string) string {
	return m.nodeAddresses[nodeID]
}

func (m *testShardManager) GetAllNodes() []string {
	nodes := make([]string, 0, len(m.nodeAddresses))
	for nodeID := range m.nodeAddresses {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

func (m *testShardManager) GetCurrentNodeID() string {
	return m.currentNodeID
}

func (m *testShardManager) GetNodeForKey(key string) string {
	if nodes := m.GetResponsibleNodes(key); len(nodes) > 0 {
		return nodes[0]
	}
	return ""
}

func (m *testShardManager) GetSuccessors(nodeID string) []string {
	return nil
}

func (m *testShardManager) GetPredecessors(nodeID string) []string {
	return nil
}

func (m *testShardManager) GetLocalNodeID() string {
	return m.currentNodeID
}

func (m *testShardManager) HasPrimaryShards() bool {
	return len(m.responsibleNodes) > 0
}
