package grub

import (
	"context"
	"errors"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/atom"
	"github.com/zoobzio/edamame"
	atomic "github.com/zoobzio/grub/internal/atomic"
	"github.com/zoobzio/soy"
)

// Default statements for common operations.
var (
	// QueryAll returns all records from the table.
	QueryAll = edamame.NewQueryStatement("query", "Query all records", edamame.QuerySpec{})

	// CountAll counts all records in the table.
	CountAll = edamame.NewAggregateStatement("count", "Count all records", edamame.AggCount, edamame.AggregateSpec{})
)

// Database provides type-safe SQL storage operations for T.
// Uses edamame internally for query building and execution.
type Database[T any] struct {
	executor   *edamame.Executor[T]
	keyCol     string
	tableName  string
	atomic     *atomic.Database[T] // lazily created via Atomic()
	atomicOnce sync.Once
}

// NewDatabase creates a Database for type T.
// keyCol specifies the column used for key-based lookups (e.g., "id").
//
// The db parameter accepts sqlx.ExtContext, which is satisfied by both *sqlx.DB and *sqlx.Tx,
// enabling transaction support by passing a transaction instead of a database connection.
func NewDatabase[T any](db sqlx.ExtContext, table, keyCol string, renderer astql.Renderer) (*Database[T], error) {
	exec, err := edamame.New[T](db, table, renderer)
	if err != nil {
		return nil, err
	}
	return &Database[T]{
		executor:  exec,
		keyCol:    keyCol,
		tableName: table,
	}, nil
}

// Get retrieves the record at key as T.
// Returns ErrNotFound if the key does not exist.
func (d *Database[T]) Get(ctx context.Context, key string) (*T, error) {
	result, err := d.executor.Soy().Select().
		Where(d.keyCol, "=", "key").
		Exec(ctx, map[string]any{"key": key})
	if err != nil {
		if errors.Is(err, soy.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return result, nil
}

// Set stores value at key (insert or update via upsert).
func (d *Database[T]) Set(ctx context.Context, _ string, value *T) error {
	s := d.executor.Soy()
	// Use InsertFull to include PK in the INSERT for proper ON CONFLICT matching
	insert := s.InsertFull().OnConflict(d.keyCol).DoUpdate()

	for _, field := range s.Metadata().Fields {
		col := field.Tags["db"]
		if col == "" || col == "-" || col == d.keyCol {
			continue
		}
		insert = insert.Set(col, col)
	}

	_, err := insert.Build().Exec(ctx, value)
	return err
}

// Delete removes the record at key.
func (d *Database[T]) Delete(ctx context.Context, key string) error {
	affected, err := d.executor.Soy().Remove().
		Where(d.keyCol, "=", "key").
		Exec(ctx, map[string]any{"key": key})
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

// Exists checks whether a record exists at key.
func (d *Database[T]) Exists(ctx context.Context, key string) (bool, error) {
	results, err := d.executor.Soy().Query().
		Where(d.keyCol, "=", "key").
		Limit(1).
		Exec(ctx, map[string]any{"key": key})
	if err != nil {
		return false, err
	}
	return len(results) > 0, nil
}

// Executor returns the underlying edamame Executor for advanced query operations.
func (d *Database[T]) Executor() *edamame.Executor[T] {
	return d.executor
}

// Query executes a query statement and returns multiple records.
func (d *Database[T]) Query(ctx context.Context, stmt edamame.QueryStatement, params map[string]any) ([]*T, error) {
	return d.executor.ExecQuery(ctx, stmt, params)
}

// Select executes a select statement and returns a single record.
func (d *Database[T]) Select(ctx context.Context, stmt edamame.SelectStatement, params map[string]any) (*T, error) {
	return d.executor.ExecSelect(ctx, stmt, params)
}

// Update executes an update statement.
func (d *Database[T]) Update(ctx context.Context, stmt edamame.UpdateStatement, params map[string]any) (*T, error) {
	return d.executor.ExecUpdate(ctx, stmt, params)
}

// Aggregate executes an aggregate statement.
func (d *Database[T]) Aggregate(ctx context.Context, stmt edamame.AggregateStatement, params map[string]any) (float64, error) {
	return d.executor.ExecAggregate(ctx, stmt, params)
}

// Atomic returns an atom-based view of this database.
// The returned atomic.Database satisfies the AtomicDatabase interface.
// The instance is created once and cached for subsequent calls.
// Panics if T is not atomizable (a programmer error).
func (d *Database[T]) Atomic() AtomicDatabase {
	d.atomicOnce.Do(func() {
		atomizer, err := atom.Use[T]()
		if err != nil {
			panic("grub: invalid type for atomization: " + err.Error())
		}
		d.atomic = atomic.New(
			d.executor,
			d.keyCol,
			d.tableName,
			atomizer.Spec(),
		)
	})
	return d.atomic
}
