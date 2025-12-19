package testing

import (
	"context"
	"testing"
	"time"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/grub"
)

// MockProvider tests

func TestMockProvider_Get(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	// Set data directly
	p.data["key1"] = []byte("value1")

	data, err := p.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("got %s, want value1", data)
	}
}

func TestMockProvider_Get_NotFound(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	_, err := p.Get(ctx, "missing")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMockProvider_Get_ReturnsCopy(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	p.data["key"] = []byte("original")

	data, _ := p.Get(ctx, "key")
	data[0] = 'X' // Mutate returned data

	// Original should be unchanged
	if string(p.data["key"]) != "original" {
		t.Error("Get should return a copy, not the original")
	}
}

func TestMockProvider_Set(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	err := p.Set(ctx, "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(p.data["key1"]) != "value1" {
		t.Errorf("data not stored correctly")
	}
}

func TestMockProvider_Set_StoresCopy(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	data := []byte("original")
	p.Set(ctx, "key", data)

	data[0] = 'X' // Mutate original

	// Stored data should be unchanged
	if string(p.data["key"]) != "original" {
		t.Error("Set should store a copy, not the original")
	}
}

func TestMockProvider_Exists(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	exists, err := p.Exists(ctx, "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected false for missing key")
	}

	p.data["present"] = []byte("data")

	exists, err = p.Exists(ctx, "present")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Error("expected true for present key")
	}
}

func TestMockProvider_Count(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	count, err := p.Count(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}

	p.data["a"] = []byte{}
	p.data["b"] = []byte{}
	p.data["c"] = []byte{}

	count, err = p.Count(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestMockProvider_List(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	p.data["key1"] = []byte{}
	p.data["key2"] = []byte{}
	p.data["key3"] = []byte{}

	keys, cursor, err := p.List(ctx, "", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}

	if cursor != "" {
		t.Errorf("expected empty cursor, got %s", cursor)
	}
}

func TestMockProvider_List_WithLimit(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		p.data[string(rune('a'+i))] = []byte{}
	}

	keys, _, err := p.List(ctx, "", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

func TestMockProvider_List_WithCursor(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	// Add keys in known order
	p.data["a"] = []byte{}
	p.data["b"] = []byte{}
	p.data["c"] = []byte{}

	// Get first page
	keys1, cursor, err := p.List(ctx, "", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(keys1) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys1))
	}

	// Use cursor to get next page
	if cursor != "" {
		keys2, _, err := p.List(ctx, cursor, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should get remaining keys
		if len(keys2) < 1 {
			t.Error("expected at least 1 more key after cursor")
		}
	}
}

func TestMockProvider_List_CursorNotFound(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	p.data["a"] = []byte{}
	p.data["b"] = []byte{}

	// Use a cursor that doesn't match any key
	keys, _, err := p.List(ctx, "nonexistent", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should start from beginning since cursor wasn't found
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

func TestMockProvider_Delete(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	p.data["key"] = []byte("data")

	err := p.Delete(ctx, "key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := p.data["key"]; ok {
		t.Error("key should be deleted")
	}
}

func TestMockProvider_Delete_NotFound(t *testing.T) {
	p := NewMockProvider()
	ctx := context.Background()

	err := p.Delete(ctx, "missing")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMockProvider_Reset(t *testing.T) {
	p := NewMockProvider()

	p.data["a"] = []byte{}
	p.data["b"] = []byte{}

	p.Reset()

	if len(p.data) != 0 {
		t.Errorf("expected empty data after reset, got %d items", len(p.data))
	}
}

func TestMockProvider_ImplementsProvider(t *testing.T) {
	var _ grub.Provider = (*MockProvider)(nil)
}

// EventCapture tests

func TestEventCapture_Handler(t *testing.T) {
	capture := NewEventCapture()
	sig := capitan.NewSignal("test.capture.handler", "Test signal")

	// Hook the handler to capitan
	l := capitan.Hook(sig, capture.Handler())

	// Emit an event
	ctx := context.Background()
	capitan.Emit(ctx, sig, capitan.NewStringKey("testkey").Field("testval"))

	// Wait for async processing
	l.Drain(ctx)
	l.Close()

	if capture.Count() != 1 {
		t.Errorf("expected 1 event, got %d", capture.Count())
	}

	events := capture.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if events[0].Signal != sig {
		t.Errorf("signal mismatch: got %v, want %v", events[0].Signal, sig)
	}

	if events[0].Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}
}

func TestEventCapture_Events(t *testing.T) {
	capture := NewEventCapture()

	events := capture.Events()
	if len(events) != 0 {
		t.Errorf("expected empty events, got %d", len(events))
	}
}

func TestEventCapture_Count(t *testing.T) {
	capture := NewEventCapture()

	if capture.Count() != 0 {
		t.Errorf("expected 0, got %d", capture.Count())
	}
}

func TestEventCapture_Reset(t *testing.T) {
	capture := NewEventCapture()

	// Manually add some events
	capture.events = append(capture.events, CapturedEvent{})
	capture.events = append(capture.events, CapturedEvent{})

	if capture.Count() != 2 {
		t.Fatalf("setup failed: expected 2 events")
	}

	capture.Reset()

	if capture.Count() != 0 {
		t.Errorf("expected 0 after reset, got %d", capture.Count())
	}
}

func TestEventCapture_WaitForCount_Immediate(t *testing.T) {
	capture := NewEventCapture()

	// Add events
	capture.events = append(capture.events, CapturedEvent{})
	capture.events = append(capture.events, CapturedEvent{})

	// Should return immediately
	result := capture.WaitForCount(2, 100*time.Millisecond)
	if !result {
		t.Error("expected true when count already reached")
	}
}

func TestEventCapture_WaitForCount_Timeout(t *testing.T) {
	capture := NewEventCapture()

	start := time.Now()
	result := capture.WaitForCount(1, 50*time.Millisecond)
	elapsed := time.Since(start)

	if result {
		t.Error("expected false on timeout")
	}

	if elapsed < 50*time.Millisecond {
		t.Error("should have waited for timeout")
	}
}

func TestEventCapture_EventsBySignal(t *testing.T) {
	capture := NewEventCapture()

	sig1 := capitan.NewSignal("test.sig1", "Signal 1")
	sig2 := capitan.NewSignal("test.sig2", "Signal 2")

	capture.events = []CapturedEvent{
		{Signal: sig1},
		{Signal: sig2},
		{Signal: sig1},
	}

	filtered := capture.EventsBySignal(sig1)
	if len(filtered) != 2 {
		t.Errorf("expected 2 events for sig1, got %d", len(filtered))
	}

	filtered = capture.EventsBySignal(sig2)
	if len(filtered) != 1 {
		t.Errorf("expected 1 event for sig2, got %d", len(filtered))
	}
}

// EventCounter tests

func TestEventCounter_Handler(t *testing.T) {
	counter := NewEventCounter()

	handler := counter.Handler()

	// Call handler directly
	handler(context.Background(), nil)
	handler(context.Background(), nil)

	if counter.Count() != 2 {
		t.Errorf("expected 2, got %d", counter.Count())
	}
}

func TestEventCounter_Count(t *testing.T) {
	counter := NewEventCounter()

	if counter.Count() != 0 {
		t.Errorf("expected 0, got %d", counter.Count())
	}
}

func TestEventCounter_Reset(t *testing.T) {
	counter := NewEventCounter()

	counter.count = 5

	counter.Reset()

	if counter.Count() != 0 {
		t.Errorf("expected 0 after reset, got %d", counter.Count())
	}
}

func TestEventCounter_WaitForCount_Immediate(t *testing.T) {
	counter := NewEventCounter()
	counter.count = 3

	result := counter.WaitForCount(3, 100*time.Millisecond)
	if !result {
		t.Error("expected true when count already reached")
	}
}

func TestEventCounter_WaitForCount_Timeout(t *testing.T) {
	counter := NewEventCounter()

	start := time.Now()
	result := counter.WaitForCount(1, 50*time.Millisecond)
	elapsed := time.Since(start)

	if result {
		t.Error("expected false on timeout")
	}

	if elapsed < 50*time.Millisecond {
		t.Error("should have waited for timeout")
	}
}

// FieldExtractor tests

func TestFieldExtractor_GetString(t *testing.T) {
	extractor := NewFieldExtractor()
	key := capitan.NewStringKey("testkey")
	fields := []capitan.Field{key.Field("testvalue")}

	val, ok := extractor.GetString(fields, key)
	if !ok {
		t.Error("expected ok to be true")
	}
	if val != "testvalue" {
		t.Errorf("expected testvalue, got %s", val)
	}
}

func TestFieldExtractor_GetInt(t *testing.T) {
	extractor := NewFieldExtractor()
	key := capitan.NewIntKey("testkey")
	fields := []capitan.Field{key.Field(42)}

	val, ok := extractor.GetInt(fields, key)
	if !ok {
		t.Error("expected ok to be true")
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}
}

func TestFieldExtractor_GetInt64(t *testing.T) {
	extractor := NewFieldExtractor()
	key := capitan.NewInt64Key("testkey")
	fields := []capitan.Field{key.Field(int64(123456789))}

	val, ok := extractor.GetInt64(fields, key)
	if !ok {
		t.Error("expected ok to be true")
	}
	if val != 123456789 {
		t.Errorf("expected 123456789, got %d", val)
	}
}

func TestFieldExtractor_GetBool(t *testing.T) {
	extractor := NewFieldExtractor()
	key := capitan.NewBoolKey("testkey")
	fields := []capitan.Field{key.Field(true)}

	val, ok := extractor.GetBool(fields, key)
	if !ok {
		t.Error("expected ok to be true")
	}
	if !val {
		t.Error("expected true")
	}
}

func TestFieldExtractor_GetDuration(t *testing.T) {
	extractor := NewFieldExtractor()
	key := capitan.NewDurationKey("testkey")
	fields := []capitan.Field{key.Field(5 * time.Second)}

	val, ok := extractor.GetDuration(fields, key)
	if !ok {
		t.Error("expected ok to be true")
	}
	if val != 5*time.Second {
		t.Errorf("expected 5s, got %v", val)
	}
}
