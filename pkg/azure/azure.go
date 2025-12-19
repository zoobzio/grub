// Package azure provides a grub Provider implementation for Azure Blob Storage.
package azure

import (
	"context"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/zoobzio/grub"
)

// Provider implements grub.Provider for Azure Blob Storage.
type Provider struct {
	client    *azblob.Client
	container string
	prefix    string
}

// New creates an Azure Blob provider scoped to the given container and key prefix.
// All operations will use keys prefixed with this value.
func New(client *azblob.Client, containerName, prefix string) *Provider {
	return &Provider{
		client:    client,
		container: containerName,
		prefix:    prefix,
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
	resp, err := p.client.DownloadStream(ctx, p.container, p.prefixKey(key), nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, grub.ErrNotFound
		}
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

// Set stores raw bytes at the given key.
func (p *Provider) Set(ctx context.Context, key string, data []byte) error {
	_, err := p.client.UploadBuffer(ctx, p.container, p.prefixKey(key), data, nil)
	return err
}

// Exists checks whether a key exists.
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	_, err := p.client.DownloadStream(ctx, p.container, p.prefixKey(key), &azblob.DownloadStreamOptions{
		Range: azblob.HTTPRange{Offset: 0, Count: 1}, // Only fetch 1 byte to check existence
	})
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Count returns the total number of blobs with the provider's prefix.
// Note: This lists all matching blobs and may be slow for large datasets.
func (p *Provider) Count(ctx context.Context) (int64, error) {
	var count int64

	pager := p.client.NewListBlobsFlatPager(p.container, &azblob.ListBlobsFlatOptions{
		Prefix: &p.prefix,
	})

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return 0, err
		}
		count += int64(len(resp.Segment.BlobItems))
	}

	return count, nil
}

// List returns a paginated list of keys with the provider's prefix.
// The cursor should be empty for the first page, or the value returned from the previous call.
func (p *Provider) List(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	opts := &azblob.ListBlobsFlatOptions{
		Prefix:     &p.prefix,
		MaxResults: int32Ptr(int32(limit)),
	}
	if cursor != "" {
		opts.Marker = &cursor
	}

	pager := p.client.NewListBlobsFlatPager(p.container, opts)

	if !pager.More() {
		return nil, "", nil
	}

	resp, err := pager.NextPage(ctx)
	if err != nil {
		return nil, "", err
	}

	// Extract and strip prefix from keys
	keys := make([]string, len(resp.Segment.BlobItems))
	for i, blob := range resp.Segment.BlobItems {
		keys[i] = p.stripPrefix(*blob.Name)
	}

	// Return next cursor if more results exist
	var nextCursor string
	if resp.NextMarker != nil && *resp.NextMarker != "" {
		nextCursor = *resp.NextMarker
	}

	return keys, nextCursor, nil
}

// Delete removes the blob at the given key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	// Check existence first to return ErrNotFound
	exists, err := p.Exists(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return grub.ErrNotFound
	}

	_, err = p.client.DeleteBlob(ctx, p.container, p.prefixKey(key), nil)
	return err
}

func int32Ptr(v int32) *int32 {
	return &v
}

// Connect is a no-op as Azure clients are stateless HTTP clients.
func (p *Provider) Connect(_ context.Context) error {
	return nil
}

// Close is a no-op as Azure clients do not require closing.
func (p *Provider) Close(_ context.Context) error {
	return nil
}

// Health checks Azure connectivity by fetching container properties.
func (p *Provider) Health(ctx context.Context) error {
	_, err := p.client.ServiceClient().NewContainerClient(p.container).GetProperties(ctx, nil)
	return err
}

// Ensure Provider implements grub.Provider and grub.Lifecycle.
var (
	_ grub.Provider  = (*Provider)(nil)
	_ grub.Lifecycle = (*Provider)(nil)
)
