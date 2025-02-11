package storage

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrEmptyKey    = errors.New("key cannot be empty")
	ErrNilValue    = errors.New("value cannot be nil")
)

// StorageItem represents a value with its content type
type StorageItem struct {
	Data        []byte
	ContentType string
}

// Store defines the interface for key-value storage operations
type Store interface {
	Get(key string) ([]byte, string, error)
	Put(key string, data []byte, contentType string) error
	Delete(key string) error
}

// Store represents a thread-safe in-memory key-value store
type StoreStruct struct {
	mu   sync.RWMutex
	data map[string]*StorageItem
}

// NewStore creates a new instance of Store
func NewStore() *StoreStruct {
	return &StoreStruct{
		data: make(map[string]*StorageItem),
	}
}

// Put stores a value with its content type for a given key
func (s *StoreStruct) Put(key string, data []byte, contentType string) error {
	if key == "" {
		return ErrEmptyKey
	}
	if data == nil {
		return ErrNilValue
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a copy of the data to prevent external modifications
	valueCopy := make([]byte, len(data))
	copy(valueCopy, data)

	s.data[key] = &StorageItem{
		Data:        valueCopy,
		ContentType: contentType,
	}
	return nil
}

// Get retrieves the data and its content type for a given key
func (s *StoreStruct) Get(key string) ([]byte, string, error) {
	if key == "" {
		return nil, "", ErrEmptyKey
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.data[key]
	if !exists {
		return nil, "", ErrKeyNotFound
	}

	// Return a copy to prevent external modifications
	valueCopy := make([]byte, len(item.Data))
	copy(valueCopy, item.Data)

	return valueCopy, item.ContentType, nil
}

// Delete removes a key-value pair
func (s *StoreStruct) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[key]; !exists {
		return ErrKeyNotFound
	}

	delete(s.data, key)
	return nil
}

// Keys returns all keys in the store
func (s *StoreStruct) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}
