// Package pgvector provides a grub VectorProvider implementation for PostgreSQL with pgvector.
package pgvector

import "github.com/zoobzio/astql"

// Distance metric constants for pgvector operations.
const (
	// L2 represents Euclidean (L2) distance - pgvector operator <->.
	L2 = "l2"

	// Cosine represents cosine distance - pgvector operator <=>.
	Cosine = "cosine"

	// InnerProduct represents negative inner product - pgvector operator <#>.
	InnerProduct = "inner_product"
)

// distanceOperator maps distance metric strings to ASTQL operators.
func distanceOperator(metric string) astql.Operator {
	switch metric {
	case Cosine:
		return astql.VectorCosineDistance
	case InnerProduct:
		return astql.VectorInnerProduct
	default:
		return astql.VectorL2Distance
	}
}
