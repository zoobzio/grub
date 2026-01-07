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

// Database provides type-safe SQL storage operations for T.
// Uses edamame internally for query building and execution.
type Database[T any] struct {
	factory    *edamame.Factory[T]
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
	f, err := edamame.New[T](db, table, renderer)
	if err != nil {
		return nil, err
	}
	return &Database[T]{
		factory:   f,
		keyCol:    keyCol,
		tableName: table,
	}, nil
}

// Get retrieves the record at key as T.
// Returns ErrNotFound if the key does not exist.
func (d *Database[T]) Get(ctx context.Context, key string) (*T, error) {
	result, err := d.factory.Soy().Select().
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
	soy := d.factory.Soy()
	// Use InsertFull to include PK in the INSERT for proper ON CONFLICT matching
	insert := soy.InsertFull().OnConflict(d.keyCol).DoUpdate()

	for _, field := range soy.Metadata().Fields {
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
	affected, err := d.factory.Soy().Remove().
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
	results, err := d.factory.Soy().Query().
		Where(d.keyCol, "=", "key").
		Limit(1).
		Exec(ctx, map[string]any{"key": key})
	if err != nil {
		return false, err
	}
	return len(results) > 0, nil
}

// Factory returns the underlying edamame Factory for query capabilities.
func (d *Database[T]) Factory() *edamame.Factory[T] {
	return d.factory
}

// Query executes a named query capability and returns multiple records.
func (d *Database[T]) Query(ctx context.Context, name string, params map[string]any) ([]*T, error) {
	return d.factory.ExecQuery(ctx, name, params)
}

// Select executes a named select capability and returns a single record.
func (d *Database[T]) Select(ctx context.Context, name string, params map[string]any) (*T, error) {
	return d.factory.ExecSelect(ctx, name, params)
}

// Update executes a named update capability.
func (d *Database[T]) Update(ctx context.Context, name string, params map[string]any) (*T, error) {
	return d.factory.ExecUpdate(ctx, name, params)
}

// Aggregate executes a named aggregate capability.
func (d *Database[T]) Aggregate(ctx context.Context, name string, params map[string]any) (any, error) {
	return d.factory.ExecAggregate(ctx, name, params)
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
			d.factory,
			d.keyCol,
			d.tableName,
			atomizer.Spec(),
		)
	})
	return d.atomic
}
