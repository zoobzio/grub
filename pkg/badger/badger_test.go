package badger

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/zoobzio/grub"
)

func setupBadger(t *testing.T) *badger.DB {
	t.Helper()

	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open badger db: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func TestProvider_Get(t *testing.T) {
	db := setupBadger(t)
	prefix := "test-get:"
	provider := New(db, prefix)

	ctx := context.Background()

	// Set data directly
	db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(prefix+"key1"), []byte(`{"test":"data"}`))
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
	db := setupBadger(t)
	provider := New(db, "test-notfound:")

	ctx := context.Background()

	_, err := provider.Get(ctx, "nonexistent")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestProvider_Set(t *testing.T) {
	db := setupBadger(t)
	prefix := "test-set:"
	provider := New(db, prefix)

	ctx := context.Background()

	err := provider.Set(ctx, "key1", []byte(`{"test":"data"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify via direct access
	var result []byte
	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(prefix + "key1"))
		if err != nil {
			return err
		}
		result, err = item.ValueCopy(nil)
		return err
	})

	if string(result) != `{"test":"data"}` {
		t.Errorf("unexpected data: %s", result)
	}
}

func TestProvider_Exists(t *testing.T) {
	db := setupBadger(t)
	prefix := "test-exists:"
	provider := New(db, prefix)

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
	db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(prefix+"key1"), []byte("data"))
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
	db := setupBadger(t)
	prefix := "test-delete:"
	provider := New(db, prefix)

	ctx := context.Background()

	// Set a key
	db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(prefix+"key1"), []byte("data"))
	})

	// Delete via provider
	err := provider.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify deleted
	var exists bool
	db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(prefix + "key1"))
		exists = err == nil
		return nil
	})

	if exists {
		t.Error("expected key to be deleted")
	}
}

func TestProvider_Delete_NotFound(t *testing.T) {
	db := setupBadger(t)
	provider := New(db, "test-delete-notfound:")

	ctx := context.Background()

	err := provider.Delete(ctx, "nonexistent")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestProvider_Count(t *testing.T) {
	db := setupBadger(t)
	prefix := "test-count:"
	provider := New(db, prefix)

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
	db.Update(func(txn *badger.Txn) error {
		txn.Set([]byte(prefix+"key1"), []byte("data1"))
		txn.Set([]byte(prefix+"key2"), []byte("data2"))
		txn.Set([]byte(prefix+"key3"), []byte("data3"))
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
	db := setupBadger(t)
	prefix := "test-list:"
	provider := New(db, prefix)

	ctx := context.Background()

	// Add some keys
	db.Update(func(txn *badger.Txn) error {
		txn.Set([]byte(prefix+"key1"), []byte("data1"))
		txn.Set([]byte(prefix+"key2"), []byte("data2"))
		txn.Set([]byte(prefix+"key3"), []byte("data3"))
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
	db := setupBadger(t)
	prefix := "test-list-page:"
	provider := New(db, prefix)

	ctx := context.Background()

	// Add many keys
	db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 15; i++ {
			txn.Set([]byte(prefix+"key"+string(rune('A'+i))), []byte("data"))
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
	db := setupBadger(t)
	provider := New(db, "test-roundtrip:")

	ctx := context.Background()

	// Test complete CRUD cycle
	data := []byte(`{"id":"123","name":"Test"}`)

	// Create
	err := provider.Set(ctx, "record1", data)
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

func TestProvider_Isolation(t *testing.T) {
	db := setupBadger(t)

	// Create two providers with different prefixes
	provider1 := New(db, "prefix1:")
	provider2 := New(db, "prefix2:")

	ctx := context.Background()

	// Set key in provider1
	provider1.Set(ctx, "key1", []byte("data1"))

	// Set same key name in provider2
	provider2.Set(ctx, "key1", []byte("data2"))

	// Verify isolation
	data1, _ := provider1.Get(ctx, "key1")
	data2, _ := provider2.Get(ctx, "key1")

	if string(data1) != "data1" {
		t.Errorf("provider1 data mismatch: got %s, want data1", data1)
	}
	if string(data2) != "data2" {
		t.Errorf("provider2 data mismatch: got %s, want data2", data2)
	}

	// Count should be independent
	count1, _ := provider1.Count(ctx)
	count2, _ := provider2.Count(ctx)

	if count1 != 1 {
		t.Errorf("provider1 count mismatch: got %d, want 1", count1)
	}
	if count2 != 1 {
		t.Errorf("provider2 count mismatch: got %d, want 1", count2)
	}
}
