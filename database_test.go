package grub

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	astqlsqlite "github.com/zoobzio/astql/pkg/sqlite"
	"github.com/zoobzio/edamame"
	"github.com/zoobzio/sentinel"
	_ "modernc.org/sqlite"
)

func init() {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
}

// TestDBUser is the model used for database tests.
type TestDBUser struct {
	ID    int    `db:"id" constraints:"primarykey"`
	Email string `db:"email" constraints:"notnull,unique"`
	Name  string `db:"name" constraints:"notnull"`
	Age   *int   `db:"age"`
}

var testDBConn *sqlx.DB
var testDBRenderer astql.Renderer

func TestMain(m *testing.M) {
	var err error
	testDBConn, err = sqlx.Connect("sqlite", ":memory:")
	if err != nil {
		panic("failed to connect to sqlite: " + err.Error())
	}

	testDBRenderer = astqlsqlite.New()

	// Create test table
	_, err = testDBConn.Exec(`
		CREATE TABLE test_users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			email TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL,
			age INTEGER
		)
	`)
	if err != nil {
		panic("failed to create table: " + err.Error())
	}

	code := m.Run()

	_ = testDBConn.Close()

	os.Exit(code)
}

func resetDBTable(t *testing.T) {
	t.Helper()
	_, err := testDBConn.Exec(`DELETE FROM test_users`)
	if err != nil {
		t.Fatalf("failed to reset table: %v", err)
	}
	// Reset auto-increment
	_, _ = testDBConn.Exec(`DELETE FROM sqlite_sequence WHERE name='test_users'`)
}

func intPtr(i int) *int {
	return &i
}

func TestNewDatabase(t *testing.T) {
	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}
	if db == nil {
		t.Fatal("NewDatabase returned nil")
	}
	if db.factory == nil {
		t.Error("factory not set")
	}
	if db.keyCol != "id" {
		t.Errorf("keyCol mismatch: got %q", db.keyCol)
	}
	if db.tableName != "test_users" {
		t.Errorf("tableName mismatch: got %q", db.tableName)
	}
}

func TestDatabase_Get(t *testing.T) {
	resetDBTable(t)
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	t.Run("existing key", func(t *testing.T) {
		_, err := testDBConn.Exec(`INSERT INTO test_users (id, email, name, age) VALUES (1, 'test@example.com', 'Test User', 25)`)
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		defer func() { _, _ = testDBConn.Exec(`DELETE FROM test_users WHERE id = 1`) }()

		user, err := db.Get(ctx, "1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if user.Email != "test@example.com" {
			t.Errorf("unexpected Email: %q", user.Email)
		}
		if user.Name != "Test User" {
			t.Errorf("unexpected Name: %q", user.Name)
		}
		if user.Age == nil || *user.Age != 25 {
			t.Errorf("unexpected Age: %v", user.Age)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := db.Get(ctx, "999")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestDatabase_Set(t *testing.T) {
	resetDBTable(t)
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	t.Run("insert new", func(t *testing.T) {
		user := &TestDBUser{
			ID:    1,
			Email: "new@example.com",
			Name:  "New User",
			Age:   intPtr(30),
		}

		err := db.Set(ctx, "1", user)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Verify
		got, err := db.Get(ctx, "1")
		if err != nil {
			t.Fatalf("Get after Set failed: %v", err)
		}
		if got.Email != "new@example.com" {
			t.Errorf("Email mismatch: got %q", got.Email)
		}
	})

	t.Run("update existing", func(t *testing.T) {
		user := &TestDBUser{
			ID:    1,
			Email: "updated@example.com",
			Name:  "Updated User",
			Age:   intPtr(35),
		}

		err := db.Set(ctx, "1", user)
		if err != nil {
			t.Fatalf("Set (update) failed: %v", err)
		}

		// Verify
		got, err := db.Get(ctx, "1")
		if err != nil {
			t.Fatalf("Get after update failed: %v", err)
		}
		if got.Email != "updated@example.com" {
			t.Errorf("Email mismatch: got %q", got.Email)
		}
	})
}

func TestDatabase_Delete(t *testing.T) {
	resetDBTable(t)
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	t.Run("existing key", func(t *testing.T) {
		_, err := testDBConn.Exec(`INSERT INTO test_users (id, email, name, age) VALUES (1, 'delete@example.com', 'Delete Me', 40)`)
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}

		err = db.Delete(ctx, "1")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err = db.Get(ctx, "1")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got %v", err)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		err := db.Delete(ctx, "999")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestDatabase_Exists(t *testing.T) {
	resetDBTable(t)
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	_, _ = testDBConn.Exec(`INSERT INTO test_users (id, email, name, age) VALUES (1, 'exists@example.com', 'Exists', 50)`)

	t.Run("existing key", func(t *testing.T) {
		exists, err := db.Exists(ctx, "1")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("expected key to exist")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		exists, err := db.Exists(ctx, "999")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("expected key to not exist")
		}
	})
}

func TestDatabase_Factory(t *testing.T) {
	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	factory := db.Factory()
	if factory == nil {
		t.Error("Factory returned nil")
	}
}

func TestDatabase_Query(t *testing.T) {
	resetDBTable(t)
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Insert test data
	_, _ = testDBConn.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('alice@example.com', 'Alice', 25),
		('bob@example.com', 'Bob', 30),
		('carol@example.com', 'Carol', 35)
	`)

	t.Run("default query", func(t *testing.T) {
		users, err := db.Query(ctx, "query", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(users) != 3 {
			t.Errorf("expected 3 users, got %d", len(users))
		}
	})

	t.Run("custom query", func(t *testing.T) {
		err := db.Factory().AddQuery(edamame.QueryCapability{
			Name:        "by-min-age",
			Description: "Users with age >= min_age",
			Spec: edamame.QuerySpec{
				Where: []edamame.ConditionSpec{
					{Field: "age", Operator: ">=", Param: "min_age"},
				},
				OrderBy: []edamame.OrderBySpec{
					{Field: "age", Direction: "asc"},
				},
			},
		})
		if err != nil {
			t.Fatalf("AddQuery failed: %v", err)
		}

		users, err := db.Query(ctx, "by-min-age", map[string]any{"min_age": 30})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(users) != 2 {
			t.Errorf("expected 2 users (age >= 30), got %d", len(users))
		}
	})
}

func TestDatabase_Select(t *testing.T) {
	resetDBTable(t)
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	_, _ = testDBConn.Exec(`INSERT INTO test_users (email, name, age) VALUES ('select@example.com', 'Select User', 45)`)

	err = db.Factory().AddSelect(edamame.SelectCapability{
		Name:        "by-email",
		Description: "Find user by email",
		Spec: edamame.SelectSpec{
			Where: []edamame.ConditionSpec{
				{Field: "email", Operator: "=", Param: "email"},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddSelect failed: %v", err)
	}

	user, err := db.Select(ctx, "by-email", map[string]any{"email": "select@example.com"})
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if user.Name != "Select User" {
		t.Errorf("expected name 'Select User', got %q", user.Name)
	}
}

func TestDatabase_Update(t *testing.T) {
	resetDBTable(t)
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	_, _ = testDBConn.Exec(`INSERT INTO test_users (email, name, age) VALUES ('update@example.com', 'Original', 20)`)

	err = db.Factory().AddUpdate(edamame.UpdateCapability{
		Name:        "rename-by-email",
		Description: "Update user name by email",
		Spec: edamame.UpdateSpec{
			Set: map[string]string{
				"name": "new_name",
			},
			Where: []edamame.ConditionSpec{
				{Field: "email", Operator: "=", Param: "email"},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddUpdate failed: %v", err)
	}

	updated, err := db.Update(ctx, "rename-by-email", map[string]any{
		"email":    "update@example.com",
		"new_name": "Updated",
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updated.Name != "Updated" {
		t.Errorf("expected name 'Updated', got %q", updated.Name)
	}
}

func TestDatabase_Aggregate(t *testing.T) {
	resetDBTable(t)
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	_, _ = testDBConn.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('a@example.com', 'A', 10),
		('b@example.com', 'B', 20),
		('c@example.com', 'C', 30)
	`)

	t.Run("count", func(t *testing.T) {
		result, err := db.Aggregate(ctx, "count", nil)
		if err != nil {
			t.Fatalf("Aggregate failed: %v", err)
		}
		count, ok := result.(float64)
		if !ok {
			t.Fatalf("expected float64, got %T", result)
		}
		if count != 3 {
			t.Errorf("expected count 3, got %v", count)
		}
	})

	t.Run("sum", func(t *testing.T) {
		err := db.Factory().AddAggregate(edamame.AggregateCapability{
			Name:        "sum-age",
			Description: "Sum of all ages",
			Func:        edamame.AggSum,
			Spec: edamame.AggregateSpec{
				Field: "age",
			},
		})
		if err != nil {
			t.Fatalf("AddAggregate failed: %v", err)
		}

		result, err := db.Aggregate(ctx, "sum-age", nil)
		if err != nil {
			t.Fatalf("Aggregate failed: %v", err)
		}
		sum, ok := result.(float64)
		if !ok {
			t.Fatalf("expected float64, got %T", result)
		}
		if sum != 60 {
			t.Errorf("expected sum 60, got %v", sum)
		}
	})
}

func TestDatabase_Atomic(t *testing.T) {
	resetDBTable(t)
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	_, _ = testDBConn.Exec(`INSERT INTO test_users (id, email, name, age) VALUES (1, 'atomic@example.com', 'Atomic User', 55)`)

	atomic := db.Atomic()
	if atomic == nil {
		t.Fatal("Atomic returned nil")
	}

	// Verify it returns the same instance
	atomic2 := db.Atomic()
	if atomic != atomic2 {
		t.Error("Atomic should return cached instance")
	}

	// Test that atomic view works
	a, err := atomic.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Atomic Get failed: %v", err)
	}
	if a.Strings["Email"] != "atomic@example.com" {
		t.Errorf("unexpected Email: %q", a.Strings["Email"])
	}
}

func TestDatabase_RoundTrip(t *testing.T) {
	resetDBTable(t)
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](testDBConn, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	original := &TestDBUser{
		ID:    1,
		Email: "roundtrip@example.com",
		Name:  "Round Trip",
		Age:   intPtr(42),
	}

	if err := db.Set(ctx, "1", original); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	retrieved, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Email != original.Email {
		t.Errorf("Email mismatch: got %q, want %q", retrieved.Email, original.Email)
	}
	if retrieved.Name != original.Name {
		t.Errorf("Name mismatch: got %q, want %q", retrieved.Name, original.Name)
	}
	if retrieved.Age == nil || *retrieved.Age != *original.Age {
		t.Errorf("Age mismatch: got %v, want %v", retrieved.Age, original.Age)
	}
}
