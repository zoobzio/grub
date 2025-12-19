// Package s3 provides a grub Provider implementation for Amazon S3.
package s3

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/zoobzio/grub"
)

// Client defines the S3 client interface used by this provider.
// This allows for easy mocking in tests.
type Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// Provider implements grub.Provider for Amazon S3.
type Provider struct {
	client Client
	bucket string
	prefix string
}

// New creates an S3 provider scoped to the given bucket and key prefix.
// All operations will use keys prefixed with this value.
func New(client Client, bucket, prefix string) *Provider {
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
	output, err := p.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.prefixKey(key)),
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, grub.ErrNotFound
		}
		// Also check for NotFound in the error message as some S3-compatible
		// services may return different error types
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return nil, grub.ErrNotFound
		}
		return nil, err
	}
	defer output.Body.Close()

	return io.ReadAll(output.Body)
}

// Set stores raw bytes at the given key.
func (p *Provider) Set(ctx context.Context, key string, data []byte) error {
	_, err := p.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.prefixKey(key)),
		Body:   bytes.NewReader(data),
	})
	return err
}

// Exists checks whether a key exists.
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	_, err := p.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.prefixKey(key)),
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return false, nil
		}
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
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
	var continuationToken *string

	for {
		output, err := p.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(p.bucket),
			Prefix:            aws.String(p.prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return 0, err
		}

		count += int64(len(output.Contents))

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	return count, nil
}

// List returns a paginated list of keys with the provider's prefix.
// The cursor should be empty for the first page, or the value returned from the previous call.
func (p *Provider) List(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	var continuationToken *string
	if cursor != "" {
		continuationToken = aws.String(cursor)
	}

	output, err := p.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(p.bucket),
		Prefix:            aws.String(p.prefix),
		MaxKeys:           aws.Int32(int32(limit)),
		ContinuationToken: continuationToken,
	})
	if err != nil {
		return nil, "", err
	}

	// Extract and strip prefix from keys
	keys := make([]string, len(output.Contents))
	for i, obj := range output.Contents {
		keys[i] = p.stripPrefix(aws.ToString(obj.Key))
	}

	// Return next cursor if more results exist
	var nextCursor string
	if aws.ToBool(output.IsTruncated) && output.NextContinuationToken != nil {
		nextCursor = aws.ToString(output.NextContinuationToken)
	}

	return keys, nextCursor, nil
}

// Delete removes the object at the given key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	// S3 DeleteObject doesn't return an error if the key doesn't exist,
	// so we need to check existence first if we want to return ErrNotFound.
	exists, err := p.Exists(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return grub.ErrNotFound
	}

	_, err = p.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.prefixKey(key)),
	})
	return err
}

// Connect is a no-op as S3 clients are stateless HTTP clients.
func (p *Provider) Connect(_ context.Context) error {
	return nil
}

// Close is a no-op as S3 clients do not require closing.
func (p *Provider) Close(_ context.Context) error {
	return nil
}

// Health checks S3 connectivity by verifying the bucket exists.
func (p *Provider) Health(ctx context.Context) error {
	_, err := p.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(p.bucket),
	})
	return err
}

// Ensure Provider implements grub.Provider and grub.Lifecycle.
var (
	_ grub.Provider  = (*Provider)(nil)
	_ grub.Lifecycle = (*Provider)(nil)
)
