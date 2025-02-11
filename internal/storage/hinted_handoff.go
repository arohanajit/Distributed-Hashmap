package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	defaultHintRetryInterval = 1 * time.Minute
	defaultHintMaxAge        = 24 * time.Hour
	defaultHintBatchSize     = 10
)

// HintedHandoffManager manages temporary storage for failed writes
type HintedHandoffManager struct {
	mu            sync.RWMutex
	hintsDir      string
	store         Store
	replicaMgr    *ReplicaManager
	stopChan      chan struct{}
	retryInterval time.Duration
	maxAge        time.Duration
}

// HintedWrite represents a write operation that needs to be replayed
type HintedWrite struct {
	Key         string    `json:"key"`
	Data        []byte    `json:"data"`
	ContentType string    `json:"content_type"`
	TargetNode  string    `json:"target_node"`
	Timestamp   time.Time `json:"timestamp"`
	RequestID   string    `json:"request_id"`
}

// NewHintedHandoffManager creates a new instance of HintedHandoffManager
func NewHintedHandoffManager(hintsDir string, store Store, replicaMgr *ReplicaManager) (*HintedHandoffManager, error) {
	// Create hints directory if it doesn't exist
	if err := os.MkdirAll(hintsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create hints directory: %v", err)
	}

	return &HintedHandoffManager{
		hintsDir:      hintsDir,
		store:         store,
		replicaMgr:    replicaMgr,
		stopChan:      make(chan struct{}),
		retryInterval: defaultHintRetryInterval,
		maxAge:        defaultHintMaxAge,
	}, nil
}

// Start begins the hint replay process
func (hm *HintedHandoffManager) Start(ctx context.Context) {
	ticker := time.NewTicker(hm.retryInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hm.stopChan:
				ticker.Stop()
				return
			case <-ticker.C:
				hm.processHints(ctx)
			}
		}
	}()
}

// Stop stops the hint replay process
func (hm *HintedHandoffManager) Stop() {
	close(hm.stopChan)
}

// StoreHint stores a write operation for later replay
func (hm *HintedHandoffManager) StoreHint(hint *HintedWrite) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Create hint file name using timestamp and target node
	fileName := fmt.Sprintf("%d_%s_%s.hint", hint.Timestamp.UnixNano(), hint.TargetNode, hint.Key)
	filePath := filepath.Join(hm.hintsDir, fileName)

	// Marshal hint to JSON
	data, err := json.Marshal(hint)
	if err != nil {
		return fmt.Errorf("failed to marshal hint: %v", err)
	}

	// Write to file atomically
	tempFile := filePath + ".tmp"
	if err := ioutil.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write hint file: %v", err)
	}

	if err := os.Rename(tempFile, filePath); err != nil {
		os.Remove(tempFile) // Clean up temp file
		return fmt.Errorf("failed to rename hint file: %v", err)
	}

	return nil
}

// processHints attempts to replay stored hints
func (hm *HintedHandoffManager) processHints(ctx context.Context) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Read all hint files
	files, err := ioutil.ReadDir(hm.hintsDir)
	if err != nil {
		return
	}

	// Process hints in batches
	var batch []*HintedWrite
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".hint" {
			filePath := filepath.Join(hm.hintsDir, file.Name())
			hint, err := hm.readHintFile(filePath)
			if err != nil {
				continue
			}

			// Check if hint is too old
			if time.Since(hint.Timestamp) > hm.maxAge {
				os.Remove(filePath)
				continue
			}

			batch = append(batch, hint)
			if len(batch) >= defaultHintBatchSize {
				hm.replayHintBatch(ctx, batch)
				batch = batch[:0]
			}
		}
	}

	// Process remaining hints
	if len(batch) > 0 {
		hm.replayHintBatch(ctx, batch)
	}
}

// readHintFile reads and parses a hint file
func (hm *HintedHandoffManager) readHintFile(filePath string) (*HintedWrite, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var hint HintedWrite
	if err := json.Unmarshal(data, &hint); err != nil {
		return nil, err
	}

	return &hint, nil
}

// replayHintBatch attempts to replay a batch of hints
func (hm *HintedHandoffManager) replayHintBatch(ctx context.Context, hints []*HintedWrite) {
	for _, hint := range hints {
		if err := hm.replayHint(ctx, hint); err == nil {
			// Remove hint file if replay was successful
			filePath := filepath.Join(hm.hintsDir, fmt.Sprintf("%d_%s_%s.hint",
				hint.Timestamp.UnixNano(), hint.TargetNode, hint.Key))
			os.Remove(filePath)
		}
	}
}

// replayHint attempts to replay a single hint
func (hm *HintedHandoffManager) replayHint(ctx context.Context, hint *HintedWrite) error {
	// Create HTTP client with timeout
	client := &http.Client{Timeout: 10 * time.Second}
	url := fmt.Sprintf("http://%s/keys/%s", hint.TargetNode, hint.Key)

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(hint.Data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", hint.ContentType)
	req.Header.Set("X-Request-ID", hint.RequestID)

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Update replica status
	hm.replicaMgr.UpdateReplicaStatus(hint.Key, hint.TargetNode, ReplicaSuccess)
	return nil
}
