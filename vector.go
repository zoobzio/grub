package grub

import "github.com/google/uuid"

// Vector wraps payload T (metadata) with vector data for atomization.
// The entire structure is atomizable, enabling field-level operations
// on both vector data and metadata.
type Vector[T any] struct {
	ID       uuid.UUID `json:"id" atom:"id"`
	Vector   []float32 `json:"vector" atom:"vector"`
	Score    float32   `json:"score,omitempty" atom:"score"`
	Metadata T         `json:"metadata" atom:"metadata"`
}
