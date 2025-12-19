// Package mongo provides a grub Provider implementation for MongoDB.
package mongo

import (
	"context"
	"errors"

	"github.com/zoobzio/grub"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// document represents the MongoDB document structure for records.
type document struct {
	Key  string `bson:"_id"`
	Data []byte `bson:"data"`
}

// Provider implements grub.Provider for MongoDB.
type Provider struct {
	collection *mongo.Collection
}

// New creates a MongoDB provider using the given collection.
// The collection stores documents with _id as the key and data as the raw bytes.
func New(collection *mongo.Collection) *Provider {
	return &Provider{
		collection: collection,
	}
}

// Get retrieves raw bytes for the given key.
func (p *Provider) Get(ctx context.Context, key string) ([]byte, error) {
	var doc document
	err := p.collection.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, grub.ErrNotFound
		}
		return nil, err
	}
	return doc.Data, nil
}

// Set stores raw bytes at the given key.
func (p *Provider) Set(ctx context.Context, key string, data []byte) error {
	doc := document{Key: key, Data: data}
	opts := options.Replace().SetUpsert(true)
	_, err := p.collection.ReplaceOne(ctx, bson.M{"_id": key}, doc, opts)
	return err
}

// Exists checks whether a key exists.
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	count, err := p.collection.CountDocuments(ctx, bson.M{"_id": key}, options.Count().SetLimit(1))
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Count returns the total number of documents in the collection.
func (p *Provider) Count(ctx context.Context) (int64, error) {
	return p.collection.CountDocuments(ctx, bson.M{})
}

// List returns a paginated list of keys.
// The cursor should be empty for the first page, or the last key from the previous call.
func (p *Provider) List(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	filter := bson.M{}
	if cursor != "" {
		filter["_id"] = bson.M{"$gt": cursor}
	}

	opts := options.Find().
		SetProjection(bson.M{"_id": 1}).
		SetSort(bson.M{"_id": 1}).
		SetLimit(int64(limit + 1)) // Fetch one extra to check for more

	cur, err := p.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, "", err
	}
	defer cur.Close(ctx)

	var keys []string
	for cur.Next(ctx) {
		var doc struct {
			Key string `bson:"_id"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, "", err
		}
		keys = append(keys, doc.Key)
	}

	if err := cur.Err(); err != nil {
		return nil, "", err
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
	result, err := p.collection.DeleteOne(ctx, bson.M{"_id": key})
	if err != nil {
		return err
	}
	if result.DeletedCount == 0 {
		return grub.ErrNotFound
	}
	return nil
}

// Connect is a no-op as the MongoDB client is pre-configured.
func (p *Provider) Connect(_ context.Context) error {
	return nil
}

// Close disconnects the MongoDB client.
func (p *Provider) Close(ctx context.Context) error {
	return p.collection.Database().Client().Disconnect(ctx)
}

// Health checks MongoDB connectivity by pinging the server.
func (p *Provider) Health(ctx context.Context) error {
	return p.collection.Database().Client().Ping(ctx, nil)
}

// Ensure Provider implements grub.Provider and grub.Lifecycle.
var (
	_ grub.Provider  = (*Provider)(nil)
	_ grub.Lifecycle = (*Provider)(nil)
)
