package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arohanajit/Distributed-Hashmap/internal/api/rest"
	"github.com/arohanajit/Distributed-Hashmap/internal/cluster"
	"github.com/arohanajit/Distributed-Hashmap/internal/config"
	"github.com/arohanajit/Distributed-Hashmap/internal/storage"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	defaultPort     = 8080
	defaultTimeout  = 10 * time.Second
	shutdownTimeout = 30 * time.Second // Default timeout for graceful shutdown
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

	// Initialize ReReplicationManager
	// For testing purposes, we're using nil since we don't have access to the real implementation
	var reReplicationMgr *cluster.ReReplicationManager = nil

	// No need to start re-replication as it's nil

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

	// Create a ShutdownManager
	shutdownMgr := cluster.NewShutdownManager(
		clusterManager,
		server,
		discoveryMgr,
		reReplicationMgr, // This is nil, but the ShutdownManager handles that case
		shardMgr,
		store,
		logger,
		shutdownTimeout,
	)

	// Setup signal handling for graceful shutdown
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// Start server in a goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Failed to start server", zap.Error(err))
		}
	}()

	logger.Info("Server started successfully", zap.String("address", addr))

	// Wait for interrupt signal
	sig := <-signalCh
	logger.Info("Received shutdown signal", zap.String("signal", sig.String()))

	// Trigger graceful shutdown with a timeout context
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	if err := shutdownMgr.Shutdown(shutdownCtx); err != nil {
		logger.Error("Error during shutdown", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("Server shutdown completed")
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

// Adding the missing method
func (m *testShardManager) HasPrimaryShardsForNode(nodeID string) bool {
	for _, nodes := range m.responsibleNodes {
		if len(nodes) > 0 && nodes[0] == nodeID {
			return true
		}
	}
	return false
}

// Adding the missing method
func (m *testShardManager) UpdateResponsibleNodes(key string, nodes []string) {
	m.responsibleNodes[key] = nodes
}

// Mock ReplicaManager to avoid calling real one that may have different signature
type mockReplicaManager struct {
	store storage.Store
}

func (r *mockReplicaManager) ReplicateKey(ctx context.Context, key string, value []byte, contentType string, nodeIDs []string) error {
	return nil
}

// Mock ReReplicationManager for testing
type mockReReplicationManager struct {
	store           storage.Store
	shardManager    cluster.ShardManager
	failureDetector *cluster.FailureDetector
	stopCh          chan struct{}
	interval        time.Duration
}

func (rm *mockReReplicationManager) Start(ctx context.Context) {
	// Mock implementation - does nothing
}

func (rm *mockReReplicationManager) Stop() {
	// Mock implementation - does nothing
}
