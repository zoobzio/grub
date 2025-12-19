// Package testing provides test utilities for grub.
package testing

import (
	"context"
	"sync"
	"time"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/grub"
)

// MockProvider is an in-memory implementation of grub.Provider for testing.
type MockProvider struct {
	data map[string][]byte
	mu   sync.RWMutex
}

// NewMockProvider creates a new in-memory provider.
func NewMockProvider() *MockProvider {
	return &MockProvider{
		data: make(map[string][]byte),
	}
}

// Get retrieves raw bytes for the given key.
func (m *MockProvider) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.data[key]
	if !ok {
		return nil, grub.ErrNotFound
	}

	// Return a copy to prevent mutation
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

// Set stores raw bytes at the given key.
func (m *MockProvider) Set(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Store a copy to prevent mutation
	stored := make([]byte, len(data))
	copy(stored, data)
	m.data[key] = stored
	return nil
}

// Exists checks whether a key exists.
func (m *MockProvider) Exists(_ context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.data[key]
	return ok, nil
}

// Count returns the total number of keys.
func (m *MockProvider) Count(_ context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return int64(len(m.data)), nil
}

// List returns a paginated list of keys.
func (m *MockProvider) List(_ context.Context, cursor string, limit int) ([]string, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Collect all keys
	allKeys := make([]string, 0, len(m.data))
	for k := range m.data {
		allKeys = append(allKeys, k)
	}

	// Find starting position based on cursor
	start := 0
	if cursor != "" {
		for i, k := range allKeys {
			if k == cursor {
				start = i + 1
				break
			}
		}
	}

	// Return slice with limit
	end := start + limit
	if end > len(allKeys) {
		end = len(allKeys)
	}

	result := allKeys[start:end]

	// Determine next cursor
	var nextCursor string
	if end < len(allKeys) {
		nextCursor = allKeys[end-1]
	}

	return result, nextCursor, nil
}

// Delete removes the record at the given key.
func (m *MockProvider) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[key]; !ok {
		return grub.ErrNotFound
	}

	delete(m.data, key)
	return nil
}

// Reset clears all data in the mock provider.
func (m *MockProvider) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data = make(map[string][]byte)
}

// Connect is a no-op for the mock provider.
func (m *MockProvider) Connect(_ context.Context) error {
	return nil
}

// Close is a no-op for the mock provider.
func (m *MockProvider) Close(_ context.Context) error {
	return nil
}

// Health always returns nil for the mock provider.
func (m *MockProvider) Health(_ context.Context) error {
	return nil
}

// Ensure MockProvider implements grub.Provider and grub.Lifecycle.
var (
	_ grub.Provider  = (*MockProvider)(nil)
	_ grub.Lifecycle = (*MockProvider)(nil)
)

// CapturedEvent represents an event captured during testing.
type CapturedEvent struct {
	Signal    capitan.Signal
	Fields    []capitan.Field
	Timestamp time.Time
}

// EventCapture captures grub events for verification in tests.
type EventCapture struct {
	events []CapturedEvent
	mu     sync.Mutex
}

// NewEventCapture creates a new event capture utility.
func NewEventCapture() *EventCapture {
	return &EventCapture{
		events: make([]CapturedEvent, 0),
	}
}

// Handler returns a capitan.EventCallback that captures events.
func (c *EventCapture) Handler() capitan.EventCallback {
	return func(_ context.Context, e *capitan.Event) {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.events = append(c.events, CapturedEvent{
			Signal:    e.Signal(),
			Fields:    e.Fields(),
			Timestamp: time.Now(),
		})
	}
}

// Events returns a copy of all captured events.
func (c *EventCapture) Events() []CapturedEvent {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make([]CapturedEvent, len(c.events))
	copy(result, c.events)
	return result
}

// Count returns the number of captured events.
func (c *EventCapture) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.events)
}

// Reset clears all captured events.
func (c *EventCapture) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.events = make([]CapturedEvent, 0)
}

// WaitForCount blocks until the specified number of events are captured or timeout.
func (c *EventCapture) WaitForCount(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Count() >= n {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return c.Count() >= n
}

// EventsBySignal returns events filtered by signal.
func (c *EventCapture) EventsBySignal(sig capitan.Signal) []CapturedEvent {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make([]CapturedEvent, 0)
	for _, e := range c.events {
		if e.Signal == sig {
			result = append(result, e)
		}
	}
	return result
}

// EventCounter counts events without storing them.
type EventCounter struct {
	count int64
	mu    sync.Mutex
}

// NewEventCounter creates a new event counter.
func NewEventCounter() *EventCounter {
	return &EventCounter{}
}

// Handler returns a capitan.EventCallback that increments the counter.
func (c *EventCounter) Handler() capitan.EventCallback {
	return func(_ context.Context, _ *capitan.Event) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.count++
	}
}

// Count returns the current count.
func (c *EventCounter) Count() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

// Reset resets the counter to zero.
func (c *EventCounter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count = 0
}

// WaitForCount blocks until the specified count is reached or timeout.
func (c *EventCounter) WaitForCount(n int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Count() >= n {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return c.Count() >= n
}

// FieldExtractor provides typed field extraction from captured events.
type FieldExtractor struct{}

// NewFieldExtractor creates a new field extractor.
func NewFieldExtractor() *FieldExtractor {
	return &FieldExtractor{}
}

// GetString extracts a string field from captured event fields.
func (f *FieldExtractor) GetString(fields []capitan.Field, key capitan.StringKey) (string, bool) {
	return key.ExtractFromFields(fields), true
}

// GetInt extracts an int field from captured event fields.
func (f *FieldExtractor) GetInt(fields []capitan.Field, key capitan.IntKey) (int, bool) {
	return key.ExtractFromFields(fields), true
}

// GetInt64 extracts an int64 field from captured event fields.
func (f *FieldExtractor) GetInt64(fields []capitan.Field, key capitan.Int64Key) (int64, bool) {
	return key.ExtractFromFields(fields), true
}

// GetBool extracts a bool field from captured event fields.
func (f *FieldExtractor) GetBool(fields []capitan.Field, key capitan.BoolKey) (bool, bool) {
	return key.ExtractFromFields(fields), true
}

// GetDuration extracts a duration field from captured event fields.
func (f *FieldExtractor) GetDuration(fields []capitan.Field, key capitan.DurationKey) (time.Duration, bool) {
	return key.ExtractFromFields(fields), true
}
