package grub

import (
	"context"
	"fmt"
	"time"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/sentinel"
)

// Service provides typed CRUD operations over a Provider.
type Service[T any] struct {
	provider Provider
	codec    Codec
	key      capitan.GenericKey[T]
	metadata sentinel.Metadata
}

// New creates a Service for type T using the given provider.
// Uses JSONCodec by default; override with WithCodec.
func New[T any](provider Provider, opts ...Option[T]) *Service[T] {
	meta := sentinel.Inspect[T]()
	variant := capitan.Variant(meta.PackageName + "." + meta.TypeName)

	svc := &Service[T]{
		provider: provider,
		codec:    JSONCodec{},
		key:      capitan.NewKey[T]("record", variant),
		metadata: meta,
	}

	for _, opt := range opts {
		opt(svc)
	}

	if svc.codec == nil {
		svc.codec = JSONCodec{}
	}

	return svc
}

// Key returns the capitan key for extracting T from events.
func (s *Service[T]) Key() capitan.GenericKey[T] {
	return s.key
}

// Metadata returns the sentinel metadata for type T.
func (s *Service[T]) Metadata() sentinel.Metadata {
	return s.metadata
}

// Get retrieves a record by key.
// Returns ErrNotFound if the key does not exist.
func (s *Service[T]) Get(ctx context.Context, key string) (*T, error) {
	start := time.Now()
	capitan.Emit(ctx, GetStarted, FieldKey.Field(key))

	data, err := s.provider.Get(ctx, key)
	if err != nil {
		capitan.Emit(ctx, GetFailed,
			FieldKey.Field(key),
			FieldError.Field(err),
			FieldDuration.Field(time.Since(start)),
		)
		return nil, err
	}

	var result T
	if err := s.codec.Unmarshal(data, &result); err != nil {
		decodeErr := fmt.Errorf("%w: %w", ErrDecode, err)
		capitan.Emit(ctx, GetFailed,
			FieldKey.Field(key),
			FieldError.Field(decodeErr),
			FieldDuration.Field(time.Since(start)),
		)
		return nil, decodeErr
	}

	capitan.Emit(ctx, GetCompleted,
		FieldKey.Field(key),
		FieldDuration.Field(time.Since(start)),
		s.key.Field(result),
	)

	return &result, nil
}

// Set stores a record at the given key.
func (s *Service[T]) Set(ctx context.Context, key string, val T) error {
	start := time.Now()
	capitan.Emit(ctx, SetStarted, FieldKey.Field(key))

	data, err := s.codec.Marshal(val)
	if err != nil {
		encodeErr := fmt.Errorf("%w: %w", ErrEncode, err)
		capitan.Emit(ctx, SetFailed,
			FieldKey.Field(key),
			FieldError.Field(encodeErr),
			FieldDuration.Field(time.Since(start)),
		)
		return encodeErr
	}

	if err := s.provider.Set(ctx, key, data); err != nil {
		capitan.Emit(ctx, SetFailed,
			FieldKey.Field(key),
			FieldError.Field(err),
			FieldDuration.Field(time.Since(start)),
		)
		return err
	}

	capitan.Emit(ctx, SetCompleted,
		FieldKey.Field(key),
		FieldDuration.Field(time.Since(start)),
		s.key.Field(val),
	)

	return nil
}

// Exists checks whether a key exists.
func (s *Service[T]) Exists(ctx context.Context, key string) (bool, error) {
	start := time.Now()

	exists, err := s.provider.Exists(ctx, key)
	if err != nil {
		return false, err
	}

	capitan.Emit(ctx, ExistsCompleted,
		FieldKey.Field(key),
		FieldExists.Field(exists),
		FieldDuration.Field(time.Since(start)),
	)

	return exists, nil
}

// Count returns the total number of records.
func (s *Service[T]) Count(ctx context.Context) (int64, error) {
	start := time.Now()

	count, err := s.provider.Count(ctx)
	if err != nil {
		return 0, err
	}

	capitan.Emit(ctx, CountCompleted,
		FieldCount.Field(count),
		FieldDuration.Field(time.Since(start)),
	)

	return count, nil
}

// List returns a paginated list of keys.
// Pass empty cursor for the first page.
func (s *Service[T]) List(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	start := time.Now()

	keys, nextCursor, err := s.provider.List(ctx, cursor, limit)
	if err != nil {
		return nil, "", err
	}

	capitan.Emit(ctx, ListCompleted,
		FieldCursor.Field(cursor),
		FieldLimit.Field(limit),
		FieldKeys.Field(keys),
		FieldDuration.Field(time.Since(start)),
	)

	return keys, nextCursor, nil
}

// Delete removes a record by key.
// Returns ErrNotFound if the key does not exist.
func (s *Service[T]) Delete(ctx context.Context, key string) error {
	start := time.Now()
	capitan.Emit(ctx, DeleteStarted, FieldKey.Field(key))

	if err := s.provider.Delete(ctx, key); err != nil {
		capitan.Emit(ctx, DeleteFailed,
			FieldKey.Field(key),
			FieldError.Field(err),
			FieldDuration.Field(time.Since(start)),
		)
		return err
	}

	capitan.Emit(ctx, DeleteCompleted,
		FieldKey.Field(key),
		FieldDuration.Field(time.Since(start)),
	)

	return nil
}
