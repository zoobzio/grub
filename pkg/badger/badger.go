// Package badger provides a grub Provider implementation for BadgerDB.
package badger

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/zoobzio/grub"
)

// Provider implements grub.Provider for BadgerDB.
type Provider struct {
	db     *badger.DB
	prefix []byte
}

// New creates a BadgerDB provider scoped to the given key prefix.
func New(db *badger.DB, prefix string) *Provider {
	return &Provider{
		db:     db,
		prefix: []byte(prefix),
	}
}

// prefixKey adds the provider's prefix to a key.
func (p *Provider) prefixKey(key string) []byte {
	return append(p.prefix, []byte(key)...)
}

// stripPrefix removes the provider's prefix from a key.
func (p *Provider) stripPrefix(key []byte) string {
	if len(key) >= len(p.prefix) {
		return string(key[len(p.prefix):])
	}
	return string(key)
}

// Get retrieves raw bytes for the given key.
func (p *Provider) Get(_ context.Context, key string) ([]byte, error) {
	var result []byte

	err := p.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(p.prefixKey(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return grub.ErrNotFound
			}
			return err
		}

		result, err = item.ValueCopy(nil)
		return err
	})

	return result, err
}

// Set stores raw bytes at the given key.
func (p *Provider) Set(_ context.Context, key string, data []byte) error {
	return p.db.Update(func(txn *badger.Txn) error {
		return txn.Set(p.prefixKey(key), data)
	})
}

// Exists checks whether a key exists.
func (p *Provider) Exists(_ context.Context, key string) (bool, error) {
	var exists bool

	err := p.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(p.prefixKey(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		exists = true
		return nil
	})

	return exists, err
}

// Count returns the total number of keys with the provider's prefix.
func (p *Provider) Count(_ context.Context) (int64, error) {
	var count int64

	err := p.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = p.prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	return count, err
}

// List returns a paginated list of keys with the provider's prefix.
// The cursor should be empty for the first page, or the last key from the previous call.
func (p *Provider) List(_ context.Context, cursor string, limit int) ([]string, string, error) {
	var keys []string
	var nextCursor string

	err := p.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = p.prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		// Start from cursor position or beginning
		if cursor == "" {
			it.Rewind()
		} else {
			it.Seek(p.prefixKey(cursor))
			// Move past cursor key
			if it.Valid() && p.stripPrefix(it.Item().Key()) == cursor {
				it.Next()
			}
		}

		// Collect keys up to limit
		for it.Valid() && len(keys) < limit {
			key := p.stripPrefix(it.Item().KeyCopy(nil))
			keys = append(keys, key)
			it.Next()
		}

		// Set next cursor if more keys exist
		if it.Valid() && len(keys) > 0 {
			nextCursor = keys[len(keys)-1]
		}

		return nil
	})

	return keys, nextCursor, err
}

// Delete removes the record at the given key.
func (p *Provider) Delete(_ context.Context, key string) error {
	prefixedKey := p.prefixKey(key)

	return p.db.Update(func(txn *badger.Txn) error {
		// Check if key exists first
		_, err := txn.Get(prefixedKey)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return grub.ErrNotFound
			}
			return err
		}

		return txn.Delete(prefixedKey)
	})
}

// Connect is a no-op as the BadgerDB database is pre-configured.
func (p *Provider) Connect(_ context.Context) error {
	return nil
}

// Close closes the BadgerDB database.
func (p *Provider) Close(_ context.Context) error {
	return p.db.Close()
}

// Health checks BadgerDB connectivity by performing a read transaction.
func (p *Provider) Health(_ context.Context) error {
	return p.db.View(func(_ *badger.Txn) error {
		return nil
	})
}

// Ensure Provider implements grub.Provider and grub.Lifecycle.
var (
	_ grub.Provider  = (*Provider)(nil)
	_ grub.Lifecycle = (*Provider)(nil)
)
