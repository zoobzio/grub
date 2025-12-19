package grub

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/zoobzio/capitan"
)

// mockProvider is an in-memory provider for testing.
type mockProvider struct {
	data     map[string][]byte
	mu       sync.RWMutex
	getErr   error
	setErr   error
	delErr   error
	existErr error
	countErr error
	listErr  error
}

func newMockProvider() *mockProvider {
	return &mockProvider{
		data: make(map[string][]byte),
	}
}

func (m *mockProvider) Get(_ context.Context, key string) ([]byte, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

func (m *mockProvider) Set(_ context.Context, key string, data []byte) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stored := make([]byte, len(data))
	copy(stored, data)
	m.data[key] = stored
	return nil
}

func (m *mockProvider) Exists(_ context.Context, key string) (bool, error) {
	if m.existErr != nil {
		return false, m.existErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[key]
	return ok, nil
}

func (m *mockProvider) Count(_ context.Context) (int64, error) {
	if m.countErr != nil {
		return 0, m.countErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.data)), nil
}

func (m *mockProvider) List(_ context.Context, cursor string, limit int) ([]string, string, error) {
	if m.listErr != nil {
		return nil, "", m.listErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}

	start := 0
	if cursor != "" {
		for i, k := range keys {
			if k == cursor {
				start = i + 1
				break
			}
		}
	}

	end := start + limit
	if end > len(keys) {
		end = len(keys)
	}

	result := keys[start:end]
	var nextCursor string
	if end < len(keys) {
		nextCursor = keys[end-1]
	}

	return result, nextCursor, nil
}

func (m *mockProvider) Delete(_ context.Context, key string) error {
	if m.delErr != nil {
		return m.delErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; !ok {
		return ErrNotFound
	}
	delete(m.data, key)
	return nil
}

// failingCodec always returns errors for testing error paths.
type failingCodec struct {
	marshalErr   error
	unmarshalErr error
}

func (f failingCodec) Marshal(_ any) ([]byte, error) {
	if f.marshalErr != nil {
		return nil, f.marshalErr
	}
	return nil, errors.New("marshal failed")
}

func (f failingCodec) Unmarshal(_ []byte, _ any) error {
	if f.unmarshalErr != nil {
		return f.unmarshalErr
	}
	return errors.New("unmarshal failed")
}

func (f failingCodec) ContentType() string {
	return "application/octet-stream"
}

// testUser is a test record type.
type testUser struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

func TestService_New(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)

	if svc == nil {
		t.Fatal("expected non-nil service")
	}

	if svc.provider != provider {
		t.Error("provider not set correctly")
	}

	// Default codec should be JSONCodec
	if _, ok := svc.codec.(JSONCodec); !ok {
		t.Error("expected JSONCodec as default")
	}
}

func TestService_New_WithCodec(t *testing.T) {
	provider := newMockProvider()
	customCodec := failingCodec{}

	svc := New[testUser](provider, WithCodec[testUser](customCodec))

	if _, ok := svc.codec.(failingCodec); !ok {
		t.Error("expected custom codec")
	}
}

func TestService_Key(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)

	key := svc.Key()
	if key.Name() != "record" {
		t.Errorf("key name: got %s, want record", key.Name())
	}
}

func TestService_Metadata(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)

	meta := svc.Metadata()
	if meta.TypeName != "testUser" {
		t.Errorf("type name: got %s, want testUser", meta.TypeName)
	}
}

func TestService_Get(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)
	ctx := context.Background()

	// Set data directly in provider
	provider.data["user1"] = []byte(`{"id":"1","email":"test@example.com"}`)

	user, err := svc.Get(ctx, "user1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if user.ID != "1" {
		t.Errorf("ID: got %s, want 1", user.ID)
	}
	if user.Email != "test@example.com" {
		t.Errorf("Email: got %s, want test@example.com", user.Email)
	}
}

func TestService_Get_NotFound(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)
	ctx := context.Background()

	_, err := svc.Get(ctx, "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestService_Get_DecodeError(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider, WithCodec[testUser](failingCodec{}))
	ctx := context.Background()

	provider.data["bad"] = []byte(`{"id":"1"}`)

	_, err := svc.Get(ctx, "bad")
	if !errors.Is(err, ErrDecode) {
		t.Errorf("expected ErrDecode, got: %v", err)
	}
}

func TestService_Get_ProviderError(t *testing.T) {
	provider := newMockProvider()
	provider.getErr = errors.New("provider failure")
	svc := New[testUser](provider)
	ctx := context.Background()

	_, err := svc.Get(ctx, "key")
	if err == nil || err.Error() != "provider failure" {
		t.Errorf("expected provider error, got: %v", err)
	}
}

func TestService_Set(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)
	ctx := context.Background()

	user := testUser{ID: "2", Email: "user@example.com"}
	err := svc.Set(ctx, "user2", user)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify via direct provider access
	data := provider.data["user2"]
	if string(data) != `{"id":"2","email":"user@example.com"}` {
		t.Errorf("unexpected data: %s", data)
	}
}

func TestService_Set_EncodeError(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider, WithCodec[testUser](failingCodec{}))
	ctx := context.Background()

	err := svc.Set(ctx, "key", testUser{})
	if !errors.Is(err, ErrEncode) {
		t.Errorf("expected ErrEncode, got: %v", err)
	}
}

func TestService_Set_ProviderError(t *testing.T) {
	provider := newMockProvider()
	provider.setErr = errors.New("write failure")
	svc := New[testUser](provider)
	ctx := context.Background()

	err := svc.Set(ctx, "key", testUser{})
	if err == nil || err.Error() != "write failure" {
		t.Errorf("expected provider error, got: %v", err)
	}
}

func TestService_Exists(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)
	ctx := context.Background()

	// Key doesn't exist
	exists, err := svc.Exists(ctx, "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected false for missing key")
	}

	// Add key
	provider.data["present"] = []byte(`{}`)

	exists, err = svc.Exists(ctx, "present")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Error("expected true for present key")
	}
}

func TestService_Exists_ProviderError(t *testing.T) {
	provider := newMockProvider()
	provider.existErr = errors.New("exists failure")
	svc := New[testUser](provider)
	ctx := context.Background()

	_, err := svc.Exists(ctx, "key")
	if err == nil {
		t.Error("expected error")
	}
}

func TestService_Count(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)
	ctx := context.Background()

	// Empty
	count, err := svc.Count(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}

	// Add items
	provider.data["a"] = []byte(`{}`)
	provider.data["b"] = []byte(`{}`)
	provider.data["c"] = []byte(`{}`)

	count, err = svc.Count(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestService_Count_ProviderError(t *testing.T) {
	provider := newMockProvider()
	provider.countErr = errors.New("count failure")
	svc := New[testUser](provider)
	ctx := context.Background()

	_, err := svc.Count(ctx)
	if err == nil {
		t.Error("expected error")
	}
}

func TestService_List(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)
	ctx := context.Background()

	provider.data["key1"] = []byte(`{}`)
	provider.data["key2"] = []byte(`{}`)
	provider.data["key3"] = []byte(`{}`)

	keys, cursor, err := svc.List(ctx, "", 100)
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

func TestService_List_ProviderError(t *testing.T) {
	provider := newMockProvider()
	provider.listErr = errors.New("list failure")
	svc := New[testUser](provider)
	ctx := context.Background()

	_, _, err := svc.List(ctx, "", 10)
	if err == nil {
		t.Error("expected error")
	}
}

func TestService_Delete(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)
	ctx := context.Background()

	provider.data["todelete"] = []byte(`{}`)

	err := svc.Delete(ctx, "todelete")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := provider.data["todelete"]; ok {
		t.Error("key should be deleted")
	}
}

func TestService_Delete_NotFound(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)
	ctx := context.Background()

	err := svc.Delete(ctx, "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestService_Delete_ProviderError(t *testing.T) {
	provider := newMockProvider()
	provider.delErr = errors.New("delete failure")
	svc := New[testUser](provider)
	ctx := context.Background()

	provider.data["key"] = []byte(`{}`)

	err := svc.Delete(ctx, "key")
	if err == nil || err.Error() != "delete failure" {
		t.Errorf("expected provider error, got: %v", err)
	}
}

func TestService_RoundTrip(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)
	ctx := context.Background()

	original := testUser{ID: "rt", Email: "round@trip.com"}

	// Set
	err := svc.Set(ctx, "roundtrip", original)
	if err != nil {
		t.Fatalf("Set error: %v", err)
	}

	// Exists
	exists, err := svc.Exists(ctx, "roundtrip")
	if err != nil {
		t.Fatalf("Exists error: %v", err)
	}
	if !exists {
		t.Error("expected key to exist")
	}

	// Get
	retrieved, err := svc.Get(ctx, "roundtrip")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if *retrieved != original {
		t.Errorf("round trip failed: got %+v, want %+v", retrieved, original)
	}

	// Count
	count, err := svc.Count(ctx)
	if err != nil {
		t.Fatalf("Count error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}

	// Delete
	err = svc.Delete(ctx, "roundtrip")
	if err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	// Verify deleted
	exists, _ = svc.Exists(ctx, "roundtrip")
	if exists {
		t.Error("expected key to be deleted")
	}
}

// Signal emission tests

func TestService_Get_EmitsSignals(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)

	var gotStarted, gotCompleted bool
	var mu sync.Mutex

	l1 := capitan.Hook(GetStarted, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		gotStarted = true
		mu.Unlock()
	})
	l2 := capitan.Hook(GetCompleted, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		gotCompleted = true
		mu.Unlock()
	})

	provider.data["key"] = []byte(`{"id":"1","email":"e@mail.com"}`)
	ctx := context.Background()

	_, err := svc.Get(ctx, "key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for async events to be processed
	_ = l1.Drain(ctx)
	_ = l2.Drain(ctx)
	l1.Close()
	l2.Close()

	mu.Lock()
	defer mu.Unlock()

	if !gotStarted {
		t.Error("expected GetStarted signal")
	}
	if !gotCompleted {
		t.Error("expected GetCompleted signal")
	}
}

func TestService_Get_EmitsFailedSignal(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)

	var gotFailed bool
	var mu sync.Mutex

	hook := func(_ context.Context, e *capitan.Event) {
		mu.Lock()
		if e.Signal() == GetFailed {
			gotFailed = true
		}
		mu.Unlock()
	}

	l := capitan.Hook(GetFailed, hook)

	ctx := context.Background()
	_, _ = svc.Get(ctx, "missing")

	// Wait for async events
	_ = l.Drain(ctx)
	l.Close()

	mu.Lock()
	defer mu.Unlock()

	if !gotFailed {
		t.Error("expected GetFailed signal")
	}
}

func TestService_Set_EmitsSignals(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)

	var gotStarted, gotCompleted bool
	var mu sync.Mutex

	l1 := capitan.Hook(SetStarted, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		gotStarted = true
		mu.Unlock()
	})
	l2 := capitan.Hook(SetCompleted, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		gotCompleted = true
		mu.Unlock()
	})

	ctx := context.Background()
	_ = svc.Set(ctx, "key", testUser{ID: "1"})

	// Wait for async events
	_ = l1.Drain(ctx)
	_ = l2.Drain(ctx)
	l1.Close()
	l2.Close()

	mu.Lock()
	defer mu.Unlock()

	if !gotStarted {
		t.Error("expected SetStarted signal")
	}
	if !gotCompleted {
		t.Error("expected SetCompleted signal")
	}
}

func TestService_Delete_EmitsSignals(t *testing.T) {
	provider := newMockProvider()
	svc := New[testUser](provider)

	var gotStarted, gotCompleted bool
	var mu sync.Mutex

	l1 := capitan.Hook(DeleteStarted, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		gotStarted = true
		mu.Unlock()
	})
	l2 := capitan.Hook(DeleteCompleted, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		gotCompleted = true
		mu.Unlock()
	})

	provider.data["key"] = []byte(`{}`)
	ctx := context.Background()
	_ = svc.Delete(ctx, "key")

	// Wait for async events
	_ = l1.Drain(ctx)
	_ = l2.Drain(ctx)
	l1.Close()
	l2.Close()

	mu.Lock()
	defer mu.Unlock()

	if !gotStarted {
		t.Error("expected DeleteStarted signal")
	}
	if !gotCompleted {
		t.Error("expected DeleteCompleted signal")
	}
}
