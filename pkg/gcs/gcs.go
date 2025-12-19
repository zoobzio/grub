// Package gcs provides a grub Provider implementation for Google Cloud Storage.
package gcs

import (
	"context"
	"errors"
	"io"

	"cloud.google.com/go/storage"
	"github.com/zoobzio/grub"
	"google.golang.org/api/iterator"
)

// Provider implements grub.Provider for Google Cloud Storage.
type Provider struct {
	client *storage.Client
	bucket string
	prefix string
}

// New creates a GCS provider scoped to the given bucket and key prefix.
// All operations will use keys prefixed with this value.
func New(client *storage.Client, bucket, prefix string) *Provider {
	return &Provider{
		client: client,
		bucket: bucket,
		prefix: prefix,
	}
}

// prefixKey adds the provider's prefix to a key.
func (p *Provider) prefixKey(key string) string {
	return p.prefix + key
}

// stripPrefix removes the provider's prefix from a key.
func (p *Provider) stripPrefix(key string) string {
	if len(key) >= len(p.prefix) {
		return key[len(p.prefix):]
	}
	return key
}

// Get retrieves raw bytes for the given key.
func (p *Provider) Get(ctx context.Context, key string) ([]byte, error) {
	obj := p.client.Bucket(p.bucket).Object(p.prefixKey(key))
	reader, err := obj.NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, grub.ErrNotFound
		}
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// Set stores raw bytes at the given key.
func (p *Provider) Set(ctx context.Context, key string, data []byte) error {
	obj := p.client.Bucket(p.bucket).Object(p.prefixKey(key))
	writer := obj.NewWriter(ctx)

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return err
	}

	return writer.Close()
}

// Exists checks whether a key exists.
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	obj := p.client.Bucket(p.bucket).Object(p.prefixKey(key))
	_, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Count returns the total number of objects with the provider's prefix.
// Note: This lists all matching objects and may be slow for large datasets.
func (p *Provider) Count(ctx context.Context) (int64, error) {
	var count int64

	it := p.client.Bucket(p.bucket).Objects(ctx, &storage.Query{
		Prefix: p.prefix,
	})

	for {
		_, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}

// List returns a paginated list of keys with the provider's prefix.
// The cursor is used internally for pagination via the iterator.
func (p *Provider) List(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	query := &storage.Query{
		Prefix: p.prefix,
	}

	it := p.client.Bucket(p.bucket).Objects(ctx, query)

	// Skip to cursor position if provided
	if cursor != "" {
		for {
			attrs, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return nil, "", err
			}
			if attrs.Name == cursor {
				break
			}
		}
	}

	// Collect keys up to limit
	var keys []string
	var lastKey string
	for len(keys) < limit {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, "", err
		}
		keys = append(keys, p.stripPrefix(attrs.Name))
		lastKey = attrs.Name
	}

	// Check if more results exist
	var nextCursor string
	_, err := it.Next()
	if err == nil {
		nextCursor = lastKey
	} else if !errors.Is(err, iterator.Done) {
		return nil, "", err
	}

	return keys, nextCursor, nil
}

// Delete removes the object at the given key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	obj := p.client.Bucket(p.bucket).Object(p.prefixKey(key))

	// Check existence first to return ErrNotFound
	_, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return grub.ErrNotFound
		}
		return err
	}

	return obj.Delete(ctx)
}

// Connect is a no-op as the GCS client is pre-configured.
func (p *Provider) Connect(_ context.Context) error {
	return nil
}

// Close closes the GCS client connection.
func (p *Provider) Close(_ context.Context) error {
	return p.client.Close()
}

// Health checks GCS connectivity by fetching bucket attributes.
func (p *Provider) Health(ctx context.Context) error {
	_, err := p.client.Bucket(p.bucket).Attrs(ctx)
	return err
}

// Ensure Provider implements grub.Provider and grub.Lifecycle.
var (
	_ grub.Provider  = (*Provider)(nil)
	_ grub.Lifecycle = (*Provider)(nil)
)
