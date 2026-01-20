package pgvector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/postgres"
	"github.com/zoobzio/dbml"
	"github.com/zoobzio/grub"
	"github.com/zoobzio/vecna"
)

// Config holds configuration for the pgvector provider.
type Config struct {
	// Table is the name of the table storing vectors.
	Table string

	// IDColumn is the primary key column name (default: "id").
	IDColumn string

	// VectorColumn is the pgvector column name (default: "embedding").
	VectorColumn string

	// MetadataColumn is the JSONB column name (default: "metadata").
	MetadataColumn string

	// Distance is the distance metric to use (default: L2).
	Distance string
}

// withDefaults returns a Config with default values applied.
func (c Config) withDefaults() Config {
	if c.IDColumn == "" {
		c.IDColumn = "id"
	}
	if c.VectorColumn == "" {
		c.VectorColumn = "embedding"
	}
	if c.MetadataColumn == "" {
		c.MetadataColumn = "metadata"
	}
	if c.Distance == "" {
		c.Distance = L2
	}
	return c
}

// Provider implements grub.VectorProvider for pgvector.
type Provider struct {
	db       *sqlx.DB
	config   Config
	instance *astql.ASTQL
	renderer *postgres.Renderer
}

// New creates a pgvector provider with the given database connection and config.
func New(db *sqlx.DB, config Config) *Provider {
	config = config.withDefaults()

	// Build DBML schema from config - validates identifiers at construction.
	project := dbml.NewProject("pgvector")
	table := dbml.NewTable(config.Table).
		AddColumn(dbml.NewColumn(config.IDColumn, "varchar")).
		AddColumn(dbml.NewColumn(config.VectorColumn, "vector")).
		AddColumn(dbml.NewColumn(config.MetadataColumn, "jsonb"))
	project.AddTable(table)

	instance, err := astql.NewFromDBML(project)
	if err != nil {
		panic(fmt.Errorf("invalid pgvector config: %w", err))
	}

	return &Provider{
		db:       db,
		config:   config,
		instance: instance,
		renderer: postgres.New(),
	}
}

// Upsert stores or updates a vector with associated metadata.
func (p *Provider) Upsert(ctx context.Context, id uuid.UUID, vector []float32, metadata []byte) error {
	query := fmt.Sprintf(
		`INSERT INTO %q (%q, %q, %q) VALUES ($1, $2, $3)
		 ON CONFLICT (%q) DO UPDATE SET %q = $2, %q = $3`,
		p.config.Table,
		p.config.IDColumn,
		p.config.VectorColumn,
		p.config.MetadataColumn,
		p.config.IDColumn,
		p.config.VectorColumn,
		p.config.MetadataColumn,
	)

	_, err := p.db.ExecContext(ctx, query, id.String(), vectorToString(vector), metadataOrNull(metadata))
	return err
}

// UpsertBatch stores or updates multiple vectors.
func (p *Provider) UpsertBatch(ctx context.Context, vectors []grub.VectorRecord) error {
	if len(vectors) == 0 {
		return nil
	}

	// Build parameterized INSERT with ON CONFLICT.
	query := fmt.Sprintf(
		`INSERT INTO %q (%q, %q, %q) VALUES `,
		p.config.Table,
		p.config.IDColumn,
		p.config.VectorColumn,
		p.config.MetadataColumn,
	)

	args := make([]any, 0, len(vectors)*3)
	for i, v := range vectors {
		if i > 0 {
			query += ", "
		}
		paramOffset := i * 3
		query += fmt.Sprintf("($%d, $%d, $%d)", paramOffset+1, paramOffset+2, paramOffset+3)
		args = append(args, v.ID.String(), vectorToString(v.Vector), metadataOrNull(v.Metadata))
	}

	query += fmt.Sprintf(
		` ON CONFLICT (%q) DO UPDATE SET %q = EXCLUDED.%q, %q = EXCLUDED.%q`,
		p.config.IDColumn,
		p.config.VectorColumn, p.config.VectorColumn,
		p.config.MetadataColumn, p.config.MetadataColumn,
	)

	_, err := p.db.ExecContext(ctx, query, args...)
	return err
}

// Get retrieves a vector by ID.
func (p *Provider) Get(ctx context.Context, id uuid.UUID) ([]float32, *grub.VectorInfo, error) {
	query := fmt.Sprintf(
		`SELECT %q, %q::text, %q::text FROM %q WHERE %q = $1`,
		p.config.IDColumn,
		p.config.VectorColumn,
		p.config.MetadataColumn,
		p.config.Table,
		p.config.IDColumn,
	)

	var rowID string
	var vectorStr string
	var metadataStr sql.NullString

	err := p.db.QueryRowContext(ctx, query, id.String()).Scan(&rowID, &vectorStr, &metadataStr)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, grub.ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}

	vector, err := parseVector(vectorStr)
	if err != nil {
		return nil, nil, fmt.Errorf("parse vector: %w", err)
	}

	var metadata []byte
	if metadataStr.Valid && metadataStr.String != "" {
		metadata = []byte(metadataStr.String)
	}

	return vector, &grub.VectorInfo{
		ID:        id,
		Dimension: len(vector),
		Metadata:  metadata,
	}, nil
}

// Delete removes a vector by ID.
func (p *Provider) Delete(ctx context.Context, id uuid.UUID) error {
	// Check existence first.
	exists, err := p.Exists(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return grub.ErrNotFound
	}

	query := fmt.Sprintf(
		`DELETE FROM %q WHERE %q = $1`,
		p.config.Table,
		p.config.IDColumn,
	)

	_, err = p.db.ExecContext(ctx, query, id.String())
	return err
}

// DeleteBatch removes multiple vectors by ID.
func (p *Provider) DeleteBatch(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}

	// Build DELETE with IN clause using positional parameters.
	query := fmt.Sprintf(
		`DELETE FROM %q WHERE %q = ANY($1::varchar[])`,
		p.config.Table,
		p.config.IDColumn,
	)

	// Convert []uuid.UUID to []string for PostgreSQL array format.
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = id.String()
	}
	arrayStr := stringArrayToPgArray(strs)

	_, err := p.db.ExecContext(ctx, query, arrayStr)
	return err
}

// Search performs similarity search and returns the k nearest neighbors.
func (p *Provider) Search(ctx context.Context, vector []float32, k int, filter map[string]any) ([]grub.VectorResult, error) {
	distOpStr := operatorString(distanceOperator(p.config.Distance))

	// Build base query with distance calculation.
	query := fmt.Sprintf(
		`SELECT %q, %q::text, %q::text, %q %s $1 AS score FROM %q`,
		p.config.IDColumn,
		p.config.VectorColumn,
		p.config.MetadataColumn,
		p.config.VectorColumn,
		distOpStr,
		p.config.Table,
	)

	args := []any{vectorToString(vector)}
	paramIdx := 2

	if len(filter) > 0 {
		query += " WHERE "
		first := true
		for field, value := range filter {
			if !first {
				query += " AND "
			}
			query += fmt.Sprintf(`%q->>'%s' = $%d`, p.config.MetadataColumn, field, paramIdx)
			args = append(args, fmt.Sprintf("%v", value))
			paramIdx++
			first = false
		}
	}

	query += fmt.Sprintf(` ORDER BY %q %s $1 ASC LIMIT %d`,
		p.config.VectorColumn, distOpStr, k)

	return p.executeSearch(ctx, query, args)
}

// Query performs similarity search with vecna filter support.
func (p *Provider) Query(ctx context.Context, vector []float32, k int, filter *vecna.Filter) ([]grub.VectorResult, error) {
	distOpStr := operatorString(distanceOperator(p.config.Distance))

	// Build base query with distance calculation.
	query := fmt.Sprintf(
		`SELECT %q, %q::text, %q::text, %q %s $1 AS score FROM %q`,
		p.config.IDColumn,
		p.config.VectorColumn,
		p.config.MetadataColumn,
		p.config.VectorColumn,
		distOpStr,
		p.config.Table,
	)

	args := []any{vectorToString(vector)}
	paramIdx := 2

	// Apply vecna filter if provided.
	if filter != nil {
		whereClause, filterArgs, nextIdx, err := translateFilter(filter, p.config.MetadataColumn, paramIdx)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			query += " WHERE " + whereClause
			args = append(args, filterArgs...)
			paramIdx = nextIdx
		}
	}

	query += fmt.Sprintf(` ORDER BY %q %s $1 ASC LIMIT %d`,
		p.config.VectorColumn, distOpStr, k)

	return p.executeSearch(ctx, query, args)
}

// executeSearch runs a search query and returns results.
func (p *Provider) executeSearch(ctx context.Context, query string, args []any) ([]grub.VectorResult, error) {
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []grub.VectorResult
	for rows.Next() {
		var id, vectorStr string
		var metadataStr sql.NullString
		var score float32
		if err := rows.Scan(&id, &vectorStr, &metadataStr, &score); err != nil {
			return nil, err
		}

		vec, err := parseVector(vectorStr)
		if err != nil {
			return nil, fmt.Errorf("parse vector: %w", err)
		}

		var metadata []byte
		if metadataStr.Valid && metadataStr.String != "" {
			metadata = []byte(metadataStr.String)
		}

		parsedID, err := uuid.Parse(id)
		if err != nil {
			return nil, err
		}
		results = append(results, grub.VectorResult{
			ID:       parsedID,
			Vector:   vec,
			Metadata: metadata,
			Score:    score,
		})
	}

	return results, rows.Err()
}

// Filter returns vectors matching the metadata filter without similarity search.
// Results are ordered by ID descending.
func (p *Provider) Filter(ctx context.Context, filter *vecna.Filter, limit int) ([]grub.VectorResult, error) {
	query := fmt.Sprintf(
		`SELECT %q, %q::text, %q::text FROM %q`,
		p.config.IDColumn,
		p.config.VectorColumn,
		p.config.MetadataColumn,
		p.config.Table,
	)

	var args []any
	paramIdx := 1

	// Apply vecna filter if provided.
	if filter != nil {
		whereClause, filterArgs, nextIdx, err := translateFilter(filter, p.config.MetadataColumn, paramIdx)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			query += " WHERE " + whereClause
			args = append(args, filterArgs...)
			paramIdx = nextIdx
		}
	}

	query += fmt.Sprintf(` ORDER BY %q DESC`, p.config.IDColumn)

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []grub.VectorResult
	for rows.Next() {
		var id, vectorStr string
		var metadataStr sql.NullString
		if err := rows.Scan(&id, &vectorStr, &metadataStr); err != nil {
			return nil, err
		}

		vec, err := parseVector(vectorStr)
		if err != nil {
			return nil, fmt.Errorf("parse vector: %w", err)
		}

		var metadata []byte
		if metadataStr.Valid && metadataStr.String != "" {
			metadata = []byte(metadataStr.String)
		}

		parsedID, err := uuid.Parse(id)
		if err != nil {
			return nil, err
		}
		results = append(results, grub.VectorResult{
			ID:       parsedID,
			Vector:   vec,
			Metadata: metadata,
		})
	}

	return results, rows.Err()
}

// List returns vector IDs.
func (p *Provider) List(ctx context.Context, limit int) ([]uuid.UUID, error) {
	query := fmt.Sprintf(
		`SELECT %q FROM %q`,
		p.config.IDColumn,
		p.config.Table,
	)

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var idStr string
		if err := rows.Scan(&idStr); err != nil {
			return nil, err
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, rows.Err()
}

// Exists checks whether a vector ID exists.
func (p *Provider) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	query := fmt.Sprintf(
		`SELECT 1 FROM %q WHERE %q = $1 LIMIT 1`,
		p.config.Table,
		p.config.IDColumn,
	)

	var one int
	err := p.db.QueryRowContext(ctx, query, id.String()).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// vectorToString converts a float32 slice to pgvector string format.
func vectorToString(v []float32) string {
	if len(v) == 0 {
		return "[]"
	}

	result := "["
	for i, f := range v {
		if i > 0 {
			result += ","
		}
		result += fmt.Sprintf("%g", f)
	}
	result += "]"
	return result
}

// parseVector parses a pgvector string to float32 slice.
func parseVector(s string) ([]float32, error) {
	if s == "" || s == "[]" {
		return nil, nil
	}

	// pgvector format: [1.0,2.0,3.0]
	var floats []float32
	if err := json.Unmarshal([]byte(s), &floats); err != nil {
		return nil, err
	}
	return floats, nil
}

// metadataOrNull returns metadata or nil for SQL NULL.
func metadataOrNull(metadata []byte) any {
	if len(metadata) == 0 {
		return nil
	}
	return string(metadata)
}

// operatorString returns the SQL string for a distance operator.
func operatorString(op astql.Operator) string {
	switch op {
	case astql.VectorCosineDistance:
		return "<=>"
	case astql.VectorInnerProduct:
		return "<#>"
	default:
		return "<->"
	}
}

// stringArrayToPgArray converts a string slice to PostgreSQL array format.
func stringArrayToPgArray(strs []string) string {
	if len(strs) == 0 {
		return "{}"
	}
	result := "{"
	for i, s := range strs {
		if i > 0 {
			result += ","
		}
		// Escape quotes in strings.
		escaped := ""
		for _, c := range s {
			if c == '"' || c == '\\' {
				escaped += "\\"
			}
			escaped += string(c)
		}
		result += `"` + escaped + `"`
	}
	result += "}"
	return result
}

// Verify Provider implements grub.VectorProvider.
var _ grub.VectorProvider = (*Provider)(nil)
