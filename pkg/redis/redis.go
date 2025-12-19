// Package redis provides a grub Provider implementation for Redis.
package redis

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zoobzio/grub"
)

// Client defines the Redis client interface used by this provider.
// This allows for easy mocking in tests.
type Client interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
	Ping(ctx context.Context) *redis.StatusCmd
	Close() error
}

// Provider implements grub.Provider for Redis.
type Provider struct {
	client Client
	prefix string
}

// New creates a Redis provider scoped to the given key prefix.
// All operations will use keys prefixed with this value.
func New(client Client, prefix string) *Provider {
	return &Provider{
		client: client,
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
	result, err := p.client.Get(ctx, p.prefixKey(key)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, grub.ErrNotFound
		}
		return nil, err
	}
	return result, nil
}

// Set stores raw bytes at the given key.
func (p *Provider) Set(ctx context.Context, key string, data []byte) error {
	return p.client.Set(ctx, p.prefixKey(key), data, 0).Err()
}

// Exists checks whether a key exists.
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	count, err := p.client.Exists(ctx, p.prefixKey(key)).Result()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Count returns the total number of keys with the provider's prefix.
// Note: This scans all matching keys and may be slow for large datasets.
func (p *Provider) Count(ctx context.Context) (int64, error) {
	var count int64
	var cursor uint64

	pattern := p.prefix + "*"

	for {
		keys, nextCursor, err := p.client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return 0, err
		}

		count += int64(len(keys))
		cursor = nextCursor

		if cursor == 0 {
			break
		}
	}

	return count, nil
}

// List returns a paginated list of keys with the provider's prefix.
// The cursor should be empty for the first page, or the value returned from the previous call.
func (p *Provider) List(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	var redisCursor uint64

	if cursor != "" {
		// Parse cursor as uint64
		var err error
		_, err = parseUint64(cursor, &redisCursor)
		if err != nil {
			return nil, "", grub.ErrInvalidKey
		}
	}

	pattern := p.prefix + "*"

	keys, nextCursor, err := p.client.Scan(ctx, redisCursor, pattern, int64(limit)).Result()
	if err != nil {
		return nil, "", err
	}

	// Strip prefix from returned keys
	result := make([]string, len(keys))
	for i, key := range keys {
		result[i] = p.stripPrefix(key)
	}

	// Return empty cursor if iteration is complete
	var nextCursorStr string
	if nextCursor != 0 {
		nextCursorStr = formatUint64(nextCursor)
	}

	return result, nextCursorStr, nil
}

// Delete removes the record at the given key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	count, err := p.client.Del(ctx, p.prefixKey(key)).Result()
	if err != nil {
		return err
	}
	if count == 0 {
		return grub.ErrNotFound
	}
	return nil
}

// parseUint64 parses a string to uint64.
func parseUint64(s string, out *uint64) (bool, error) {
	var val uint64
	for _, c := range s {
		if c < '0' || c > '9' {
			return false, errors.New("invalid cursor")
		}
		val = val*10 + uint64(c-'0')
	}
	*out = val
	return true, nil
}

// formatUint64 formats a uint64 to string.
func formatUint64(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte(v%10) + '0'
		v /= 10
	}
	return string(buf[i:])
}

// Connect is a no-op as the Redis client is pre-configured.
func (p *Provider) Connect(_ context.Context) error {
	return nil
}

// Close closes the Redis client connection.
func (p *Provider) Close(_ context.Context) error {
	return p.client.Close()
}

// Health checks Redis connectivity by sending a PING command.
func (p *Provider) Health(ctx context.Context) error {
	return p.client.Ping(ctx).Err()
}

// Ensure Provider implements grub.Provider and grub.Lifecycle.
var (
	_ grub.Provider  = (*Provider)(nil)
	_ grub.Lifecycle = (*Provider)(nil)
)
