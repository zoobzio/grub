package gcs

import (
	"context"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/zoobzio/grub"
	"google.golang.org/api/option"
)

func setupGCS(t *testing.T) *storage.Client {
	t.Helper()

	// Create a client - for unit tests we'll skip actual operations
	// Real integration tests would use a fake GCS server or testcontainers
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Skipf("skipping GCS test: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return client
}

func TestProvider_New(t *testing.T) {
	client := setupGCS(t)
	provider := New(client, "test-bucket", "prefix/")

	if provider == nil {
		t.Fatal("expected non-nil provider")
	}

	if provider.bucket != "test-bucket" {
		t.Errorf("bucket: got %s, want test-bucket", provider.bucket)
	}

	if provider.prefix != "prefix/" {
		t.Errorf("prefix: got %s, want prefix/", provider.prefix)
	}
}

func TestProvider_prefixKey(t *testing.T) {
	client := setupGCS(t)
	provider := New(client, "bucket", "prefix/")

	result := provider.prefixKey("key1")
	if result != "prefix/key1" {
		t.Errorf("got %s, want prefix/key1", result)
	}
}

func TestProvider_stripPrefix(t *testing.T) {
	client := setupGCS(t)
	provider := New(client, "bucket", "prefix/")

	result := provider.stripPrefix("prefix/key1")
	if result != "key1" {
		t.Errorf("got %s, want key1", result)
	}

	// Short key
	result = provider.stripPrefix("x")
	if result != "x" {
		t.Errorf("got %s, want x", result)
	}
}

func TestProvider_ImplementsProvider(t *testing.T) {
	var _ grub.Provider = (*Provider)(nil)
}
