// Package shared provides canonical type definitions used across grub modules.
package shared //nolint:revive // internal shared package is intentional

// ObjectInfo holds provider-level metadata for blob storage.
// Used by BucketProvider implementations.
type ObjectInfo struct {
	Key         string
	ContentType string
	Size        int64
	ETag        string
	Metadata    map[string]string
}
