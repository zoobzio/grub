package bolt

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zoobzio/grub"
	bolt "go.etcd.io/bbolt"
)

func setupBolt(t *testing.T) *bolt.DB {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt db: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
		os.Remove(path)
	})

	return db
}

func TestProvider_Get(t *testing.T) {
	db := setupBolt(t)
	provider, err := New(db, "test-get")
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	ctx := context.Background()

	// Set data directly
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test-get"))
		return b.Put([]byte("key1"), []byte(`{"test":"data"}`))
	})

	// Get via provider
	data, err := provider.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(data) != `{"test":"data"}` {
		t.Errorf("unexpected data: %s", data)
	}
}

func TestProvider_Get_NotFound(t *testing.T) {
	db := setupBolt(t)
	provider, err := New(db, "test-notfound")
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	ctx := context.Background()

	_, err = provider.Get(ctx, "nonexistent")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestProvider_Set(t *testing.T) {
	db := setupBolt(t)
	provider, err := New(db, "test-set")
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	ctx := context.Background()

	err = provider.Set(ctx, "key1", []byte(`{"test":"data"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify via direct access
	var result []byte
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test-set"))
		result = b.Get([]byte("key1"))
		return nil
	})

	if string(result) != `{"test":"data"}` {
		t.Errorf("unexpected data: %s", result)
	}
}

func TestProvider_Exists(t *testing.T) {
	db := setupBolt(t)
	provider, err := New(db, "test-exists")
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	ctx := context.Background()

	// Key doesn't exist
	exists, err := provider.Exists(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected key to not exist")
	}

	// Set the key
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test-exists"))
		return b.Put([]byte("key1"), []byte("data"))
	})

	// Key exists
	exists, err = provider.Exists(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Error("expected key to exist")
	}
}

func TestProvider_Delete(t *testing.T) {
	db := setupBolt(t)
	provider, err := New(db, "test-delete")
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	ctx := context.Background()

	// Set a key
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test-delete"))
		return b.Put([]byte("key1"), []byte("data"))
	})

	// Delete via provider
	err = provider.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify deleted
	var exists bool
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test-delete"))
		exists = b.Get([]byte("key1")) != nil
		return nil
	})

	if exists {
		t.Error("expected key to be deleted")
	}
}

func TestProvider_Delete_NotFound(t *testing.T) {
	db := setupBolt(t)
	provider, err := New(db, "test-delete-notfound")
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	ctx := context.Background()

	err = provider.Delete(ctx, "nonexistent")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestProvider_Count(t *testing.T) {
	db := setupBolt(t)
	provider, err := New(db, "test-count")
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	ctx := context.Background()

	// Initially empty
	count, err := provider.Count(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}

	// Add some keys
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test-count"))
		b.Put([]byte("key1"), []byte("data1"))
		b.Put([]byte("key2"), []byte("data2"))
		b.Put([]byte("key3"), []byte("data3"))
		return nil
	})

	count, err = provider.Count(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
}

func TestProvider_List(t *testing.T) {
	db := setupBolt(t)
	provider, err := New(db, "test-list")
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	ctx := context.Background()

	// Add some keys
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test-list"))
		b.Put([]byte("key1"), []byte("data1"))
		b.Put([]byte("key2"), []byte("data2"))
		b.Put([]byte("key3"), []byte("data3"))
		return nil
	})

	// List all keys
	keys, cursor, err := provider.List(ctx, "", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}

	// Cursor should be empty when all keys returned
	if cursor != "" {
		t.Errorf("expected empty cursor, got: %s", cursor)
	}
}

func TestProvider_List_Pagination(t *testing.T) {
	db := setupBolt(t)
	provider, err := New(db, "test-list-page")
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	ctx := context.Background()

	// Add many keys
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test-list-page"))
		for i := 0; i < 15; i++ {
			b.Put([]byte("key"+string(rune('A'+i))), []byte("data"))
		}
		return nil
	})

	// Get first page
	keys1, cursor1, err := provider.List(ctx, "", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(keys1) != 5 {
		t.Errorf("expected 5 keys in first page, got %d", len(keys1))
	}

	if cursor1 == "" {
		t.Error("expected non-empty cursor for pagination")
	}

	// Collect all keys through pagination
	allKeys := make(map[string]bool)
	for _, k := range keys1 {
		allKeys[k] = true
	}

	cursor := cursor1
	iterations := 0
	for cursor != "" && iterations < 10 {
		keys, nextCursor, err := provider.List(ctx, cursor, 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, k := range keys {
			allKeys[k] = true
		}
		cursor = nextCursor
		iterations++
	}

	if len(allKeys) != 15 {
		t.Errorf("expected 15 unique keys, got %d", len(allKeys))
	}
}

func TestProvider_RoundTrip(t *testing.T) {
	db := setupBolt(t)
	provider, err := New(db, "test-roundtrip")
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	ctx := context.Background()

	// Test complete CRUD cycle
	data := []byte(`{"id":"123","name":"Test"}`)

	// Create
	err = provider.Set(ctx, "record1", data)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Read
	result, err := provider.Get(ctx, "record1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(result) != string(data) {
		t.Errorf("data mismatch: got %s, want %s", result, data)
	}

	// Exists
	exists, err := provider.Exists(ctx, "record1")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected record to exist")
	}

	// Update
	newData := []byte(`{"id":"123","name":"Updated"}`)
	err = provider.Set(ctx, "record1", newData)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	result, _ = provider.Get(ctx, "record1")
	if string(result) != string(newData) {
		t.Errorf("update mismatch: got %s, want %s", result, newData)
	}

	// Delete
	err = provider.Delete(ctx, "record1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	exists, _ = provider.Exists(ctx, "record1")
	if exists {
		t.Error("expected record to be deleted")
	}
}
