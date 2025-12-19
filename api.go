// Package grub provides a generic, provider-agnostic CRUD interface
// for record-driven operations with optional observability via capitan.
package grub

import (
	"context"
	"errors"
)

// Semantic errors for CRUD and lifecycle operations.
var (
	ErrNotFound      = errors.New("grub: record not found")
	ErrAlreadyExists = errors.New("grub: record already exists")
	ErrInvalidKey    = errors.New("grub: invalid key")
	ErrEncode        = errors.New("grub: encoding failed")
	ErrDecode        = errors.New("grub: decoding failed")
	ErrUnsupported   = errors.New("grub: operation not supported")
)

// Provider defines raw storage operations scoped to a collection.
// Implementations are backend-specific (Redis, S3, PostgreSQL, etc.)
// and should be configured with collection context at construction.
type Provider interface {
	// Get retrieves raw bytes for the given key.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores raw bytes at the given key.
	// Creates or overwrites the record.
	Set(ctx context.Context, key string, data []byte) error

	// Exists checks whether a key exists in the collection.
	Exists(ctx context.Context, key string) (bool, error)

	// Count returns the total number of records in the collection.
	Count(ctx context.Context) (int64, error)

	// List returns a paginated list of keys in the collection.
	// Pass empty cursor for the first page.
	// Returns empty nextCursor when no more pages exist.
	List(ctx context.Context, cursor string, limit int) (keys []string, nextCursor string, err error)

	// Delete removes the record at the given key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error
}

// Codec defines serialization for record payloads.
type Codec interface {
	// Marshal serializes a value to bytes.
	Marshal(v any) ([]byte, error)

	// Unmarshal deserializes bytes into a value.
	Unmarshal(data []byte, v any) error

	// ContentType returns the MIME type for the encoding format.
	ContentType() string
}

// Lifecycle defines connection lifecycle management for providers.
// Providers that do not support a particular operation should return ErrUnsupported.
type Lifecycle interface {
	// Connect establishes the connection to the backing store.
	// Returns nil if the provider is pre-configured or connection is not required.
	Connect(ctx context.Context) error

	// Close releases resources and closes the connection.
	// Returns nil if the provider has no resources to release.
	Close(ctx context.Context) error

	// Health performs a minimal operation to verify connectivity.
	// Returns nil if the connection is healthy.
	Health(ctx context.Context) error
}
