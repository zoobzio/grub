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

	// ErrDimensionMismatch indicates the vector dimension does not match the index.
	ErrDimensionMismatch = errors.New("grub: vector dimension mismatch")

	// ErrInvalidVector indicates the provided vector is malformed or empty.
	ErrInvalidVector = errors.New("grub: invalid vector")

	// ErrIndexNotReady indicates the vector index is not ready for operations.
	ErrIndexNotReady = errors.New("grub: index not ready")

	// ErrInvalidQuery indicates the query filter contains validation errors.
	ErrInvalidQuery = errors.New("grub: invalid query filter")

	// ErrOperatorNotSupported indicates the filter operator is not supported by the provider.
	ErrOperatorNotSupported = errors.New("grub: operator not supported by provider")

	// ErrFilterNotSupported indicates the provider does not support metadata-only filtering.
	ErrFilterNotSupported = errors.New("grub: filter not supported by provider")

	// ErrNoPrimaryKey indicates no field has the primarykey constraint.
	ErrNoPrimaryKey = errors.New("grub: no primary key defined in struct tags")

	// ErrMultiplePrimaryKeys indicates multiple fields have the primarykey constraint.
	ErrMultiplePrimaryKeys = errors.New("grub: multiple primary keys not supported")
)
