package s3

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"github.com/zoobzio/grub"
)

const testBucket = "grub-test-bucket"

func setupS3(t *testing.T) *s3.Client {
	t.Helper()

	ctx := context.Background()

	container, err := localstack.Run(ctx, "localstack/localstack:latest")
	if err != nil {
		t.Fatalf("failed to start localstack: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	endpoint, err := container.PortEndpoint(ctx, "4566", "http")
	if err != nil {
		t.Fatalf("failed to get endpoint: %v", err)
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	// Create test bucket
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	return client
}

func TestProvider_Get(t *testing.T) {
	client := setupS3(t)
	prefix := "test-get/"
	provider := New(client, testBucket, prefix)

	ctx := context.Background()

	// Put object directly via S3
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(prefix + "key1"),
		Body:   strings.NewReader(`{"test":"data"}`),
	})
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

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
	client := setupS3(t)
	provider := New(client, testBucket, "test-notfound/")

	ctx := context.Background()

	_, err := provider.Get(ctx, "nonexistent")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestProvider_Set(t *testing.T) {
	client := setupS3(t)
	prefix := "test-set/"
	provider := New(client, testBucket, prefix)

	ctx := context.Background()

	err := provider.Set(ctx, "key1", []byte(`{"test":"data"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify via S3
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(prefix + "key1"),
	})
	if err != nil {
		t.Fatalf("failed to get object: %v", err)
	}
	defer output.Body.Close()

	data := make([]byte, 100)
	n, _ := output.Body.Read(data)

	if string(data[:n]) != `{"test":"data"}` {
		t.Errorf("unexpected data: %s", data[:n])
	}
}

func TestProvider_Exists(t *testing.T) {
	client := setupS3(t)
	prefix := "test-exists/"
	provider := New(client, testBucket, prefix)

	ctx := context.Background()

	// Key doesn't exist
	exists, err := provider.Exists(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected key to not exist")
	}

	// Put the object
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(prefix + "key1"),
		Body:   bytes.NewReader([]byte("data")),
	})
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

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
	client := setupS3(t)
	prefix := "test-delete/"
	provider := New(client, testBucket, prefix)

	ctx := context.Background()

	// Put an object
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(prefix + "key1"),
		Body:   bytes.NewReader([]byte("data")),
	})
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

	// Delete via provider
	err = provider.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify deleted
	exists, _ := provider.Exists(ctx, "key1")
	if exists {
		t.Error("expected object to be deleted")
	}
}

func TestProvider_Delete_NotFound(t *testing.T) {
	client := setupS3(t)
	provider := New(client, testBucket, "test-delete-notfound/")

	ctx := context.Background()

	err := provider.Delete(ctx, "nonexistent")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestProvider_Count(t *testing.T) {
	client := setupS3(t)
	prefix := "test-count/"
	provider := New(client, testBucket, prefix)

	ctx := context.Background()

	// Initially empty
	count, err := provider.Count(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}

	// Add some objects
	for i := 0; i < 3; i++ {
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(prefix + "key" + string(rune('1'+i))),
			Body:   bytes.NewReader([]byte("data")),
		})
		if err != nil {
			t.Fatalf("failed to put object: %v", err)
		}
	}

	count, err = provider.Count(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
}

func TestProvider_List(t *testing.T) {
	client := setupS3(t)
	prefix := "test-list/"
	provider := New(client, testBucket, prefix)

	ctx := context.Background()

	// Add some objects
	for i := 0; i < 3; i++ {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(prefix + "key" + string(rune('1'+i))),
			Body:   bytes.NewReader([]byte("data")),
		})
		if err != nil {
			t.Fatalf("failed to put object: %v", err)
		}
	}

	// List all keys
	keys, cursor, err := provider.List(ctx, "", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d: %v", len(keys), keys)
	}

	// Cursor should be empty when all keys returned
	if cursor != "" {
		t.Errorf("expected empty cursor, got: %s", cursor)
	}

	// Verify keys don't have prefix
	for _, key := range keys {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			t.Errorf("key should not have prefix: %s", key)
		}
	}
}

func TestProvider_List_Pagination(t *testing.T) {
	client := setupS3(t)
	prefix := "test-list-page/"
	provider := New(client, testBucket, prefix)

	ctx := context.Background()

	// Add many objects
	for i := 0; i < 15; i++ {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(prefix + "key" + string(rune('A'+i))),
			Body:   bytes.NewReader([]byte("data")),
		})
		if err != nil {
			t.Fatalf("failed to put object: %v", err)
		}
	}

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
	client := setupS3(t)
	provider := New(client, testBucket, "test-roundtrip/")

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
