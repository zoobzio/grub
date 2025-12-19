// Package bolt provides a grub Provider implementation for BoltDB (bbolt).
package bolt

import (
	"context"

	"github.com/zoobzio/grub"
	bolt "go.etcd.io/bbolt"
)

// Provider implements grub.Provider for BoltDB.
type Provider struct {
	db     *bolt.DB
	bucket []byte
}

// New creates a BoltDB provider scoped to the given bucket.
// The bucket will be created if it doesn't exist.
func New(db *bolt.DB, bucket string) (*Provider, error) {
	bucketBytes := []byte(bucket)

	// Ensure bucket exists
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketBytes)
		return err
	})
	if err != nil {
		return nil, err
	}

	return &Provider{
		db:     db,
		bucket: bucketBytes,
	}, nil
}

// Get retrieves raw bytes for the given key.
func (p *Provider) Get(_ context.Context, key string) ([]byte, error) {
	var result []byte

	err := p.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(p.bucket)
		if b == nil {
			return grub.ErrNotFound
		}

		data := b.Get([]byte(key))
		if data == nil {
			return grub.ErrNotFound
		}

		// Copy data since it's only valid within the transaction
		result = make([]byte, len(data))
		copy(result, data)
		return nil
	})

	return result, err
}

// Set stores raw bytes at the given key.
func (p *Provider) Set(_ context.Context, key string, data []byte) error {
	return p.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(p.bucket)
		if b == nil {
			return grub.ErrNotFound
		}
		return b.Put([]byte(key), data)
	})
}

// Exists checks whether a key exists.
func (p *Provider) Exists(_ context.Context, key string) (bool, error) {
	var exists bool

	err := p.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(p.bucket)
		if b == nil {
			return nil
		}
		exists = b.Get([]byte(key)) != nil
		return nil
	})

	return exists, err
}

// Count returns the total number of keys in the bucket.
func (p *Provider) Count(_ context.Context) (int64, error) {
	var count int64

	err := p.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(p.bucket)
		if b == nil {
			return nil
		}
		stats := b.Stats()
		count = int64(stats.KeyN)
		return nil
	})

	return count, err
}

// List returns a paginated list of keys in the bucket.
// The cursor should be empty for the first page, or the last key from the previous call.
func (p *Provider) List(_ context.Context, cursor string, limit int) ([]string, string, error) {
	var keys []string
	var nextCursor string

	err := p.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(p.bucket)
		if b == nil {
			return nil
		}

		c := b.Cursor()
		var k []byte

		// Start from cursor position or beginning
		if cursor == "" {
			k, _ = c.First()
		} else {
			// Seek to cursor and move to next
			k, _ = c.Seek([]byte(cursor))
			if k != nil && string(k) == cursor {
				k, _ = c.Next()
			}
		}

		// Collect keys up to limit
		for k != nil && len(keys) < limit {
			keys = append(keys, string(k))
			k, _ = c.Next()
		}

		// Set next cursor if more keys exist
		if k != nil {
			nextCursor = keys[len(keys)-1]
		}

		return nil
	})

	return keys, nextCursor, err
}

// Delete removes the record at the given key.
func (p *Provider) Delete(_ context.Context, key string) error {
	return p.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(p.bucket)
		if b == nil {
			return grub.ErrNotFound
		}

		// Check if key exists first
		if b.Get([]byte(key)) == nil {
			return grub.ErrNotFound
		}

		return b.Delete([]byte(key))
	})
}

// Connect is a no-op as the BoltDB database is pre-configured.
func (p *Provider) Connect(_ context.Context) error {
	return nil
}

// Close closes the BoltDB database.
func (p *Provider) Close(_ context.Context) error {
	return p.db.Close()
}

// Health checks BoltDB connectivity by performing a read transaction.
func (p *Provider) Health(_ context.Context) error {
	return p.db.View(func(_ *bolt.Tx) error {
		return nil
	})
}

// Ensure Provider implements grub.Provider and grub.Lifecycle.
var (
	_ grub.Provider  = (*Provider)(nil)
	_ grub.Lifecycle = (*Provider)(nil)
)
