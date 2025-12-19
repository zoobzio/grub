// Package firestore provides a grub Provider implementation for Google Cloud Firestore.
package firestore

import (
	"context"

	"cloud.google.com/go/firestore"
	"github.com/zoobzio/grub"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// document represents the Firestore document structure for records.
type document struct {
	Data []byte `firestore:"data"`
}

// Provider implements grub.Provider for Google Cloud Firestore.
type Provider struct {
	client     *firestore.Client
	collection string
}

// New creates a Firestore provider using the given collection.
// Documents are stored with the key as the document ID and data as a byte array field.
func New(client *firestore.Client, collection string) *Provider {
	return &Provider{
		client:     client,
		collection: collection,
	}
}

// Get retrieves raw bytes for the given key.
func (p *Provider) Get(ctx context.Context, key string) ([]byte, error) {
	doc, err := p.client.Collection(p.collection).Doc(key).Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, grub.ErrNotFound
		}
		return nil, err
	}

	var d document
	if err := doc.DataTo(&d); err != nil {
		return nil, err
	}

	return d.Data, nil
}

// Set stores raw bytes at the given key.
func (p *Provider) Set(ctx context.Context, key string, data []byte) error {
	_, err := p.client.Collection(p.collection).Doc(key).Set(ctx, document{Data: data})
	return err
}

// Exists checks whether a key exists.
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	doc, err := p.client.Collection(p.collection).Doc(key).Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, nil
		}
		return false, err
	}
	return doc.Exists(), nil
}

// Count returns the total number of documents in the collection.
// Note: This fetches all documents and may be slow/expensive for large datasets.
func (p *Provider) Count(ctx context.Context) (int64, error) {
	// Firestore doesn't have a native count operation without fetching docs
	// Use aggregation query if available, otherwise iterate
	docs, err := p.client.Collection(p.collection).Documents(ctx).GetAll()
	if err != nil {
		return 0, err
	}
	return int64(len(docs)), nil
}

// List returns a paginated list of keys.
// The cursor should be empty for the first page, or the last key from the previous call.
func (p *Provider) List(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	query := p.client.Collection(p.collection).OrderBy(firestore.DocumentID, firestore.Asc).Limit(limit + 1)

	if cursor != "" {
		query = query.StartAfter(cursor)
	}

	docs, err := query.Documents(ctx).GetAll()
	if err != nil {
		return nil, "", err
	}

	keys := make([]string, 0, len(docs))
	for _, doc := range docs {
		keys = append(keys, doc.Ref.ID)
	}

	// Determine next cursor
	var nextCursor string
	if len(keys) > limit {
		keys = keys[:limit]
		nextCursor = keys[limit-1]
	}

	return keys, nextCursor, nil
}

// Delete removes the document at the given key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	// Check existence first to return ErrNotFound
	exists, err := p.Exists(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return grub.ErrNotFound
	}

	_, err = p.client.Collection(p.collection).Doc(key).Delete(ctx)
	return err
}

// Connect is a no-op as the Firestore client is pre-configured.
func (p *Provider) Connect(_ context.Context) error {
	return nil
}

// Close closes the Firestore client connection.
func (p *Provider) Close(_ context.Context) error {
	return p.client.Close()
}

// Health checks Firestore connectivity by listing a single document.
func (p *Provider) Health(ctx context.Context) error {
	iter := p.client.Collection(p.collection).Limit(1).Documents(ctx)
	defer iter.Stop()
	_, err := iter.Next()
	if err == iterator.Done {
		// Empty collection is healthy
		return nil
	}
	return err
}

// Ensure Provider implements grub.Provider and grub.Lifecycle.
var (
	_ grub.Provider  = (*Provider)(nil)
	_ grub.Lifecycle = (*Provider)(nil)
)
