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