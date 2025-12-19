package redis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	redisTC "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/zoobzio/grub"
)

func setupRedis(t *testing.T) *redis.Client {
	t.Helper()

	ctx := context.Background()

	container, err := redisTC.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	opts, err := redis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("failed to parse connection string: %v", err)
	}

	client := redis.NewClient(opts)
	t.Cleanup(func() { client.Close() })

	return client
}

func TestProvider_Get(t *testing.T) {
	client := setupRedis(t)
	prefix := "test-get:"
	provider := New(client, prefix)

	ctx := context.Background()

	// Set data directly via Redis
	client.Set(ctx, prefix+"key1", []byte(`{"test":"data"}`), 0)

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
	client := setupRedis(t)
	provider := New(client, "test-notfound:")

	ctx := context.Background()

	_, err := provider.Get(ctx, "nonexistent")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestProvider_Set(t *testing.T) {
	client := setupRedis(t)
	prefix := "test-set:"
	provider := New(client, prefix)

	ctx := context.Background()

	err := provider.Set(ctx, "key1", []byte(`{"test":"data"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify via Redis
	result, err := client.Get(ctx, prefix+"key1").Result()
	if err != nil {
		t.Fatalf("failed to get from redis: %v", err)
	}

	if result != `{"test":"data"}` {
		t.Errorf("unexpected data: %s", result)
	}
}

func TestProvider_Exists(t *testing.T) {
	client := setupRedis(t)
	prefix := "test-exists:"
	provider := New(client, prefix)

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
	client.Set(ctx, prefix+"key1", "data", 0)

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
	client := setupRedis(t)
	prefix := "test-delete:"
	provider := New(client, prefix)

	ctx := context.Background()

	// Set a key
	client.Set(ctx, prefix+"key1", "data", 0)

	// Delete via provider
	err := provider.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify deleted
	exists, _ := client.Exists(ctx, prefix+"key1").Result()
	if exists > 0 {
		t.Error("expected key to be deleted")
	}
}

func TestProvider_Delete_NotFound(t *testing.T) {
	client := setupRedis(t)
	provider := New(client, "test-delete-notfound:")

	ctx := context.Background()

	err := provider.Delete(ctx, "nonexistent")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestProvider_Count(t *testing.T) {
	client := setupRedis(t)
	prefix := "test-count:"
	provider := New(client, prefix)

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
	client.Set(ctx, prefix+"key1", "data1", 0)
	client.Set(ctx, prefix+"key2", "data2", 0)
	client.Set(ctx, prefix+"key3", "data3", 0)

	count, err = provider.Count(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
}

func TestProvider_List(t *testing.T) {
	client := setupRedis(t)
	prefix := "test-list:"
	provider := New(client, prefix)

	ctx := context.Background()

	// Add some keys
	client.Set(ctx, prefix+"key1", "data1", 0)
	client.Set(ctx, prefix+"key2", "data2", 0)
	client.Set(ctx, prefix+"key3", "data3", 0)

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
		t.Logf("cursor not empty: %s (this may be expected for small datasets)", cursor)
	}

	// Verify keys don't have prefix
	for _, key := range keys {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			t.Errorf("key should not have prefix: %s", key)
		}
	}
}

func TestProvider_List_Pagination(t *testing.T) {
	client := setupRedis(t)
	prefix := "test-list-page:"
	provider := New(client, prefix)

	ctx := context.Background()

	// Add many keys
	for i := 0; i < 20; i++ {
		client.Set(ctx, prefix+"key"+string(rune('A'+i)), "data", 0)
	}

	// Get first page
	keys1, cursor1, err := provider.List(ctx, "", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("First page: %d keys, cursor: %s", len(keys1), cursor1)

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

	if len(allKeys) != 20 {
		t.Errorf("expected 20 unique keys, got %d", len(allKeys))
	}
}

func TestProvider_RoundTrip(t *testing.T) {
	client := setupRedis(t)
	provider := New(client, "test-roundtrip:")

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
