// Package milvus provides a grub VectorProvider implementation for Milvus.
package milvus

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zoobzio/grub"
	"github.com/zoobzio/vecna"
)

// Config holds configuration for the Milvus provider.
type Config struct {
	// Collection is the name of the Milvus collection.
	Collection string
	// IDField is the name of the primary key field. Defaults to "id".
	IDField string
	// VectorField is the name of the vector field. Defaults to "embedding".
	VectorField string
	// MetadataField is the name of the JSON metadata field. Defaults to "metadata".
	MetadataField string
}

// Provider implements grub.VectorProvider for Milvus.
type Provider struct {
	client client.Client
	config Config
}

// New creates a Milvus provider with the given client and config.
func New(c client.Client, config Config) *Provider {
	if config.IDField == "" {
		config.IDField = "id"
	}
	if config.VectorField == "" {
		config.VectorField = "embedding"
	}
	if config.MetadataField == "" {
		config.MetadataField = "metadata"
	}
	return &Provider{
		client: c,
		config: config,
	}
}

// Upsert stores or updates a vector with associated metadata.
func (p *Provider) Upsert(ctx context.Context, id uuid.UUID, vector []float32, metadata []byte) error {
	idCol := entity.NewColumnVarChar(p.config.IDField, []string{id.String()})
	vecCol := entity.NewColumnFloatVector(p.config.VectorField, len(vector), [][]float32{vector})
	metaCol := entity.NewColumnJSONBytes(p.config.MetadataField, [][]byte{metadata})

	_, err := p.client.Upsert(ctx, p.config.Collection, "", idCol, vecCol, metaCol)
	if err != nil {
		return err
	}

	// Flush to make data immediately searchable
	return p.client.Flush(ctx, p.config.Collection, false)
}

// UpsertBatch stores or updates multiple vectors.
func (p *Provider) UpsertBatch(ctx context.Context, vectors []grub.VectorRecord) error {
	if len(vectors) == 0 {
		return nil
	}

	ids := make([]string, len(vectors))
	vecs := make([][]float32, len(vectors))
	metas := make([][]byte, len(vectors))

	var dim int
	for i, v := range vectors {
		ids[i] = v.ID.String()
		vecs[i] = v.Vector
		if i == 0 {
			dim = len(v.Vector)
		}
		metas[i] = v.Metadata
	}

	idCol := entity.NewColumnVarChar(p.config.IDField, ids)
	vecCol := entity.NewColumnFloatVector(p.config.VectorField, dim, vecs)
	metaCol := entity.NewColumnJSONBytes(p.config.MetadataField, metas)

	_, err := p.client.Upsert(ctx, p.config.Collection, "", idCol, vecCol, metaCol)
	if err != nil {
		return err
	}

	// Flush to make data immediately searchable
	return p.client.Flush(ctx, p.config.Collection, false)
}

// Get retrieves a vector by ID.
func (p *Provider) Get(ctx context.Context, id uuid.UUID) ([]float32, *grub.VectorInfo, error) {
	idStr := id.String()
	expr := fmt.Sprintf(`%s == "%s"`, p.config.IDField, idStr)

	results, err := p.client.Query(
		ctx,
		p.config.Collection,
		nil,
		expr,
		[]string{p.config.IDField, p.config.VectorField, p.config.MetadataField},
	)
	if err != nil {
		return nil, nil, err
	}

	if len(results) == 0 {
		return nil, nil, grub.ErrNotFound
	}

	// Extract data from columns
	var vector []float32
	var metadata []byte

	for _, col := range results {
		switch col.Name() {
		case p.config.VectorField:
			if vecCol, ok := col.(*entity.ColumnFloatVector); ok {
				data := vecCol.Data()
				if len(data) > 0 {
					vector = data[0]
				}
			}
		case p.config.MetadataField:
			if metaCol, ok := col.(*entity.ColumnJSONBytes); ok {
				data := metaCol.Data()
				if len(data) > 0 {
					metadata = data[0]
				}
			}
		}
	}

	if vector == nil {
		return nil, nil, grub.ErrNotFound
	}

	return vector, &grub.VectorInfo{
		ID:        id,
		Dimension: len(vector),
		Metadata:  metadata,
	}, nil
}

// Delete removes a vector by ID.
func (p *Provider) Delete(ctx context.Context, id uuid.UUID) error {
	// Check if exists first
	exists, err := p.Exists(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return grub.ErrNotFound
	}

	expr := fmt.Sprintf(`%s in ["%s"]`, p.config.IDField, id.String())
	if err := p.client.Delete(ctx, p.config.Collection, "", expr); err != nil {
		return err
	}

	// Flush to make deletion immediately visible
	return p.client.Flush(ctx, p.config.Collection, false)
}

// DeleteBatch removes multiple vectors by ID.
func (p *Provider) DeleteBatch(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}

	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = fmt.Sprintf(`"%s"`, id.String())
	}
	expr := fmt.Sprintf(`%s in [%s]`, p.config.IDField, strings.Join(quoted, ","))

	if err := p.client.Delete(ctx, p.config.Collection, "", expr); err != nil {
		return err
	}

	// Flush to make deletion immediately visible
	return p.client.Flush(ctx, p.config.Collection, false)
}

// Search performs similarity search and returns the k nearest neighbors.
func (p *Provider) Search(ctx context.Context, vector []float32, k int, filter map[string]any) ([]grub.VectorResult, error) {
	sp, _ := entity.NewIndexFlatSearchParam()

	var expr string
	if len(filter) > 0 {
		expr = buildFilterExpr(filter, p.config.MetadataField)
	}

	results, err := p.client.Search(
		ctx,
		p.config.Collection,
		nil,
		expr,
		[]string{p.config.IDField, p.config.MetadataField},
		[]entity.Vector{entity.FloatVector(vector)},
		p.config.VectorField,
		entity.L2,
		k,
		sp,
	)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, nil
	}

	result := results[0]
	vectorResults := make([]grub.VectorResult, result.ResultCount)

	for i := 0; i < result.ResultCount; i++ {
		var id uuid.UUID
		var metadata []byte

		// Get ID
		if idCol := result.Fields.GetColumn(p.config.IDField); idCol != nil {
			if vc, ok := idCol.(*entity.ColumnVarChar); ok {
				idStr, _ := vc.ValueByIdx(i)
				id, _ = uuid.Parse(idStr)
			}
		}

		// Get metadata
		if metaCol := result.Fields.GetColumn(p.config.MetadataField); metaCol != nil {
			if jc, ok := metaCol.(*entity.ColumnJSONBytes); ok {
				metadata, _ = jc.ValueByIdx(i)
			}
		}

		vectorResults[i] = grub.VectorResult{
			ID:       id,
			Metadata: metadata,
			Score:    result.Scores[i],
		}
	}

	return vectorResults, nil
}

// Query performs similarity search with vecna filter support.
func (p *Provider) Query(ctx context.Context, vector []float32, k int, filter *vecna.Filter) ([]grub.VectorResult, error) {
	sp, _ := entity.NewIndexFlatSearchParam()

	expr, err := translateFilter(filter, p.config.MetadataField)
	if err != nil {
		return nil, err
	}

	results, err := p.client.Search(
		ctx,
		p.config.Collection,
		nil,
		expr,
		[]string{p.config.IDField, p.config.MetadataField},
		[]entity.Vector{entity.FloatVector(vector)},
		p.config.VectorField,
		entity.L2,
		k,
		sp,
	)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, nil
	}

	result := results[0]
	vectorResults := make([]grub.VectorResult, result.ResultCount)

	for i := 0; i < result.ResultCount; i++ {
		var id uuid.UUID
		var metadata []byte

		// Get ID
		if idCol := result.Fields.GetColumn(p.config.IDField); idCol != nil {
			if vc, ok := idCol.(*entity.ColumnVarChar); ok {
				idStr, _ := vc.ValueByIdx(i)
				id, _ = uuid.Parse(idStr)
			}
		}

		// Get metadata
		if metaCol := result.Fields.GetColumn(p.config.MetadataField); metaCol != nil {
			if jc, ok := metaCol.(*entity.ColumnJSONBytes); ok {
				metadata, _ = jc.ValueByIdx(i)
			}
		}

		vectorResults[i] = grub.VectorResult{
			ID:       id,
			Metadata: metadata,
			Score:    result.Scores[i],
		}
	}

	return vectorResults, nil
}

// List returns vector IDs.
func (p *Provider) List(ctx context.Context, limit int) ([]uuid.UUID, error) {
	var opts []client.SearchQueryOptionFunc
	if limit > 0 {
		opts = append(opts, client.WithLimit(int64(limit)))
	}

	results, err := p.client.Query(ctx, p.config.Collection, nil, "", []string{p.config.IDField}, opts...)
	if err != nil {
		return nil, err
	}

	var ids []uuid.UUID
	for _, col := range results {
		if col.Name() == p.config.IDField {
			if vc, ok := col.(*entity.ColumnVarChar); ok {
				for i := 0; i < vc.Len(); i++ {
					idStr, _ := vc.ValueByIdx(i)
					id, err := uuid.Parse(idStr)
					if err != nil {
						return nil, err
					}
					ids = append(ids, id)
				}
			}
		}
	}

	return ids, nil
}

// Exists checks whether a vector ID exists.
func (p *Provider) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	expr := fmt.Sprintf(`%s == "%s"`, p.config.IDField, id.String())

	results, err := p.client.Query(
		ctx,
		p.config.Collection,
		nil,
		expr,
		[]string{p.config.IDField},
		client.WithLimit(1),
	)
	if err != nil {
		return false, err
	}

	for _, col := range results {
		if col.Len() > 0 {
			return true, nil
		}
	}
	return false, nil
}

// buildFilterExpr builds a Milvus filter expression from metadata map.
func buildFilterExpr(m map[string]any, metaField string) string {
	if len(m) == 0 {
		return ""
	}

	var conditions []string
	for k, v := range m {
		switch val := v.(type) {
		case string:
			conditions = append(conditions, fmt.Sprintf(`%s["%s"] == "%s"`, metaField, k, val))
		case int, int64, float64:
			conditions = append(conditions, fmt.Sprintf(`%s["%s"] == %v`, metaField, k, val))
		case bool:
			conditions = append(conditions, fmt.Sprintf(`%s["%s"] == %t`, metaField, k, val))
		}
	}

	return strings.Join(conditions, " and ")
}
