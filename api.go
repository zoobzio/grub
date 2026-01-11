// Package grub provides a provider-agnostic storage interface.
// Atoms serve as the type-agnostic API boundary; providers store data
// in its intended structure, not as atoms.
package grub

import (
	"context"
	"time"

	"github.com/zoobzio/atom"
	"github.com/zoobzio/edamame"
	"github.com/zoobzio/grub/internal/shared"
)

// Semantic errors for storage operations (re-exported from internal/shared).
var (
	ErrNotFound        = shared.ErrNotFound
	ErrDuplicate       = shared.ErrDuplicate
	ErrConflict        = shared.ErrConflict
	ErrConstraint      = shared.ErrConstraint
	ErrInvalidKey      = shared.ErrInvalidKey
	ErrReadOnly        = shared.ErrReadOnly
	ErrTableExists     = shared.ErrTableExists
	ErrTableNotFound   = shared.ErrTableNotFound
	ErrTTLNotSupported = shared.ErrTTLNotSupported
)

// StoreProvider defines raw key-value storage operations.
// Implementations (redis, badger, bolt) satisfy this interface.
type StoreProvider interface {
	// Get retrieves the value at key.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores value at key with optional TTL.
	// TTL of 0 means no expiration.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Delete removes the value at key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Exists checks whether a key exists.
	Exists(ctx context.Context, key string) (bool, error)

	// List returns keys matching the given prefix.
	// Limit of 0 means no limit.
	List(ctx context.Context, prefix string, limit int) ([]string, error)

	// GetBatch retrieves multiple values by key.
	// Missing keys are omitted from the result (no error).
	GetBatch(ctx context.Context, keys []string) (map[string][]byte, error)

	// SetBatch stores multiple key-value pairs with optional TTL.
	// TTL of 0 means no expiration.
	SetBatch(ctx context.Context, items map[string][]byte, ttl time.Duration) error
}

// AtomicStore defines atom-based key-value storage operations.
// atomic.Store[T] satisfies this interface, enabling type-agnostic access
// for framework internals (field-level encryption, pipelines, etc.).
type AtomicStore interface {
	// Spec returns the atom spec describing the stored type's structure.
	Spec() atom.Spec

	// Get retrieves the value at key as an Atom.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) (*atom.Atom, error)

	// Set stores an Atom at key with optional TTL.
	// TTL of 0 means no expiration.
	Set(ctx context.Context, key string, data *atom.Atom, ttl time.Duration) error

	// Delete removes the value at key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Exists checks whether a key exists.
	Exists(ctx context.Context, key string) (bool, error)
}

// AtomicDatabase defines atom-based storage operations for a single table.
// atomic.Database[T] satisfies this interface, enabling type-agnostic access
// for framework internals (field-level encryption, pipelines, etc.).
type AtomicDatabase interface {
	// Table returns the table name this provider manages.
	Table() string

	// Spec returns the atom spec describing the table's structure.
	Spec() atom.Spec

	// Get retrieves the record at key as an Atom.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) (*atom.Atom, error)

	// Set stores an Atom at key (insert or update).
	Set(ctx context.Context, key string, data *atom.Atom) error

	// Delete removes the record at key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Exists checks whether a record exists at key.
	Exists(ctx context.Context, key string) (bool, error)

	// Query executes a query statement and returns atoms.
	Query(ctx context.Context, stmt edamame.QueryStatement, params map[string]any) ([]*atom.Atom, error)

	// Select executes a select statement and returns an atom.
	Select(ctx context.Context, stmt edamame.SelectStatement, params map[string]any) (*atom.Atom, error)
}

// BucketProvider defines raw blob storage operations.
// Implementations (s3, gcs, azure) satisfy this interface.
type BucketProvider interface {
	// Get retrieves the blob at key.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, *ObjectInfo, error)

	// Put stores data at key with associated metadata.
	Put(ctx context.Context, key string, data []byte, info *ObjectInfo) error

	// Delete removes the blob at key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Exists checks whether a key exists.
	Exists(ctx context.Context, key string) (bool, error)

	// List returns object info for keys matching the given prefix.
	// Limit of 0 means no limit.
	List(ctx context.Context, prefix string, limit int) ([]ObjectInfo, error)
}

// AtomicObject holds blob metadata with an atomized payload.
// Used by AtomicBucket for type-agnostic access to blob data.
type AtomicObject struct {
	Key         string
	ContentType string
	Size        int64
	ETag        string
	Metadata    map[string]string
	Data        *atom.Atom
}

// AtomicBucket defines atom-based blob storage operations.
// atomic.Bucket[T] satisfies this interface, enabling type-agnostic access
// for framework internals (field-level encryption, pipelines, etc.).
type AtomicBucket interface {
	// Spec returns the atom spec describing the payload T structure.
	Spec() atom.Spec

	// Get retrieves the blob at key with atomized payload.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) (*AtomicObject, error)

	// Put stores an object with atomized payload at key.
	Put(ctx context.Context, key string, obj *AtomicObject) error

	// Delete removes the blob at key.
	// Returns ErrNotFound if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Exists checks whether a key exists.
	Exists(ctx context.Context, key string) (bool, error)
}
