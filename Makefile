.PHONY: build test clean

# Build settings
BINARY_NAME=dhashmap
BUILD_DIR=build

# Go settings
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean

# Build targets
build:
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-server ./cmd/server
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-client ./cmd/client

# Test targets
test:
	$(GOTEST) -v ./...

# Integration test targets
test-integration:
	@echo "Running integration tests..."
	@chmod +x scripts/run_integration_tests.sh
	@./scripts/run_integration_tests.sh

# Chaos test targets
test-chaos:
	@echo "Running chaos tests..."
	@chmod +x scripts/run_chaos_tests.sh
	@./scripts/run_chaos_tests.sh

# Run all tests
test-all: test test-integration test-chaos
	@echo "All tests completed!"

# Make scripts executable
scripts-executable:
	@chmod +x scripts/*.sh

# Clean targets
clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

# Run server
run-server:
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-server ./cmd/server
	./$(BUILD_DIR)/$(BINARY_NAME)-server

# Run client
run-client:
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-client ./cmd/client
	./$(BUILD_DIR)/$(BINARY_NAME)-client 