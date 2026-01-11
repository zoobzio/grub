// Package atomic provides atom-based storage wrappers for grub.
package atomic

import (
	"context"
	"errors"

	"github.com/zoobzio/atom"
	"github.com/zoobzio/edamame"
	"github.com/zoobzio/grub/internal/shared"
	"github.com/zoobzio/soy"
)

// Database provides atom-based storage operations.
// Derived from grub.Database[T] via Atomic(), satisfies grub.AtomicDatabase interface.
type Database[T any] struct {
	executor  *edamame.Executor[T]
	keyCol    string
	tableName string
	spec      atom.Spec
}

// New creates an atomic Database wrapper.
func New[T any](executor *edamame.Executor[T], keyCol, tableName string, spec atom.Spec) *Database[T] {
	return &Database[T]{
		executor:  executor,
		keyCol:    keyCol,
		tableName: tableName,
		spec:      spec,
	}
}

// Table returns the table name.
func (d *Database[T]) Table() string {
	return d.tableName
}

// Spec returns the atom spec for this table's type.
func (d *Database[T]) Spec() atom.Spec {
	return d.spec
}

// Get retrieves the record at key as an Atom.
// Returns ErrNotFound if the key does not exist.
func (d *Database[T]) Get(ctx context.Context, key string) (*atom.Atom, error) {
	result, err := d.executor.Soy().Select().
		Where(d.keyCol, "=", "key").
		ExecAtom(ctx, map[string]any{"key": key})
	if err != nil {
		if errors.Is(err, soy.ErrNotFound) {
			return nil, shared.ErrNotFound
		}
		return nil, err
	}
	return result, nil
}

// Set stores an Atom at key (insert or update via upsert).
func (d *Database[T]) Set(ctx context.Context, _ string, data *atom.Atom) error {
	atomizer, err := atom.Use[T]()
	if err != nil {
		return err
	}
	value, err := atomizer.Deatomize(data)
	if err != nil {
		return err
	}

	s := d.executor.Soy()
	insert := s.InsertFull().OnConflict(d.keyCol).DoUpdate()

	for _, field := range s.Metadata().Fields {
		col := field.Tags["db"]
		if col == "" || col == "-" || col == d.keyCol {
			continue
		}
		insert = insert.Set(col, col)
	}

	_, err = insert.Build().Exec(ctx, value)
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
		return shared.ErrNotFound
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

// Query executes a query statement and returns atoms.
func (d *Database[T]) Query(ctx context.Context, stmt edamame.QueryStatement, params map[string]any) ([]*atom.Atom, error) {
	return d.executor.ExecQueryAtom(ctx, stmt, params)
}

// Select executes a select statement and returns an atom.
func (d *Database[T]) Select(ctx context.Context, stmt edamame.SelectStatement, params map[string]any) (*atom.Atom, error) {
	return d.executor.ExecSelectAtom(ctx, stmt, params)
}
