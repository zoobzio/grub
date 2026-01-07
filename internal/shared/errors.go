// Package shared contains canonical type definitions shared across grub.
package shared //nolint:revive // internal shared package is intentional

import "errors"

// Semantic errors for storage operations.
var (
	// ErrNotFound indicates the requested record does not exist.
	ErrNotFound = errors.New("grub: record not found")

	// ErrDuplicate indicates a record with the same key already exists.
	ErrDuplicate = errors.New("grub: duplicate record")

	// ErrConflict indicates a concurrent modification conflict.
	ErrConflict = errors.New("grub: conflict")

	// ErrConstraint indicates a constraint violation (foreign key, check, etc.).
	ErrConstraint = errors.New("grub: constraint violation")

	// ErrInvalidKey indicates the provided key is malformed or empty.
	ErrInvalidKey = errors.New("grub: invalid key")

	// ErrReadOnly indicates a write was attempted on a read-only connection.
	ErrReadOnly = errors.New("grub: read-only")

	// ErrTableExists indicates a table with the same name is already registered.
	ErrTableExists = errors.New("grub: table already registered")

	// ErrTableNotFound indicates the table is not registered.
	ErrTableNotFound = errors.New("grub: table not registered")

	// ErrTTLNotSupported indicates the provider does not support TTL.
	ErrTTLNotSupported = errors.New("grub: TTL not supported by provider")
)
