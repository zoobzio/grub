package atomic

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	astqlsqlite "github.com/zoobzio/astql/pkg/sqlite"
	"github.com/zoobzio/atom"
	"github.com/zoobzio/edamame"
	"github.com/zoobzio/grub/internal/shared"
	"github.com/zoobzio/sentinel"
	_ "modernc.org/sqlite"
)

func init() {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
}

// TestUser is the model used for database tests.
type TestUser struct {
	ID    int    `db:"id" atom:"id" constraints:"primarykey"`
	Email string `db:"email" atom:"email" constraints:"notnull,unique"`
	Name  string `db:"name" atom:"name" constraints:"notnull"`
	Age   *int   `db:"age" atom:"age"`
}

var testDB *sqlx.DB
var testRenderer astql.Renderer
var testFactory *edamame.Factory[TestUser]

func TestMain(m *testing.M) {
	var err error
	testDB, err = sqlx.Connect("sqlite", ":memory:")
	if err != nil {
		panic("failed to connect to sqlite: " + err.Error())
	}

	testRenderer = astqlsqlite.New()

	// Create test table
	_, err = testDB.Exec(`
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

	testFactory, err = edamame.New[TestUser](testDB, "test_users", testRenderer)
	if err != nil {
		panic("failed to create factory: " + err.Error())
	}

	code := m.Run()

	_ = testDB.Close()

	os.Exit(code)
}

func resetTable(t *testing.T) {
	t.Helper()
	_, err := testDB.Exec(`DELETE FROM test_users`)
	if err != nil {
		t.Fatalf("failed to reset table: %v", err)
	}
	// Reset auto-increment
	_, _ = testDB.Exec(`DELETE FROM sqlite_sequence WHERE name='test_users'`)
}

func TestNew(t *testing.T) {
	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()

	db := New[TestUser](testFactory, "id", "test_users", spec)

	if db == nil {
		t.Fatal("New returned nil")
	}
	if db.factory != testFactory {
		t.Error("factory not set correctly")
	}
	if db.keyCol != "id" {
		t.Errorf("keyCol not set correctly: %q", db.keyCol)
	}
	if db.tableName != "test_users" {
		t.Errorf("tableName not set correctly: %q", db.tableName)
	}
}

func TestDatabase_Table(t *testing.T) {
	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()

	db := New[TestUser](testFactory, "id", "test_users", spec)

	if table := db.Table(); table != "test_users" {
		t.Errorf("Table() returned %q, expected 'test_users'", table)
	}
}

func TestDatabase_Spec(t *testing.T) {
	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()

	db := New[TestUser](testFactory, "id", "test_users", spec)

	returnedSpec := db.Spec()
	if returnedSpec.TypeName != spec.TypeName {
		t.Errorf("Spec TypeName mismatch: got %q, want %q", returnedSpec.TypeName, spec.TypeName)
	}
}

func TestDatabase_Get(t *testing.T) {
	resetTable(t)
	ctx := context.Background()

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](testFactory, "id", "test_users", spec)

	t.Run("existing key", func(t *testing.T) {
		_, err := testDB.Exec(`INSERT INTO test_users (id, email, name, age) VALUES (1, 'test@example.com', 'Test User', 25)`)
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}

		result, err := db.Get(ctx, "1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if result.Strings["Email"] != "test@example.com" {
			t.Errorf("unexpected Email: %q", result.Strings["Email"])
		}
		if result.Strings["Name"] != "Test User" {
			t.Errorf("unexpected Name: %q", result.Strings["Name"])
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := db.Get(ctx, "999")
		if !errors.Is(err, shared.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestDatabase_Set(t *testing.T) {
	resetTable(t)
	ctx := context.Background()

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](testFactory, "id", "test_users", spec)

	t.Run("insert new", func(t *testing.T) {
		age := int64(30)
		a := &atom.Atom{
			Ints:    map[string]int64{"ID": 1},
			Strings: map[string]string{"Email": "new@example.com", "Name": "New User"},
			IntPtrs: map[string]*int64{"Age": &age},
		}

		err := db.Set(ctx, "1", a)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Verify
		result, err := db.Get(ctx, "1")
		if err != nil {
			t.Fatalf("Get after Set failed: %v", err)
		}
		if result.Strings["Email"] != "new@example.com" {
			t.Errorf("Email mismatch: got %q", result.Strings["Email"])
		}
	})

	t.Run("update existing", func(t *testing.T) {
		age := int64(35)
		a := &atom.Atom{
			Ints:    map[string]int64{"ID": 1},
			Strings: map[string]string{"Email": "updated@example.com", "Name": "Updated User"},
			IntPtrs: map[string]*int64{"Age": &age},
		}

		err := db.Set(ctx, "1", a)
		if err != nil {
			t.Fatalf("Set (update) failed: %v", err)
		}

		// Verify
		result, err := db.Get(ctx, "1")
		if err != nil {
			t.Fatalf("Get after update failed: %v", err)
		}
		if result.Strings["Email"] != "updated@example.com" {
			t.Errorf("Email mismatch: got %q", result.Strings["Email"])
		}
	})
}

func TestDatabase_Delete(t *testing.T) {
	resetTable(t)
	ctx := context.Background()

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](testFactory, "id", "test_users", spec)

	t.Run("existing key", func(t *testing.T) {
		_, err := testDB.Exec(`INSERT INTO test_users (id, email, name, age) VALUES (1, 'delete@example.com', 'Delete Me', 40)`)
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}

		err = db.Delete(ctx, "1")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err = db.Get(ctx, "1")
		if !errors.Is(err, shared.ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got %v", err)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		err := db.Delete(ctx, "999")
		if !errors.Is(err, shared.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestDatabase_Exists(t *testing.T) {
	resetTable(t)
	ctx := context.Background()

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](testFactory, "id", "test_users", spec)

	_, _ = testDB.Exec(`INSERT INTO test_users (id, email, name, age) VALUES (1, 'exists@example.com', 'Exists', 50)`)

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

func TestDatabase_RoundTrip(t *testing.T) {
	resetTable(t)
	ctx := context.Background()

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](testFactory, "id", "test_users", spec)

	age := int64(42)
	original := &atom.Atom{
		Ints:    map[string]int64{"ID": 1},
		Strings: map[string]string{"Email": "roundtrip@example.com", "Name": "Round Trip"},
		IntPtrs: map[string]*int64{"Age": &age},
	}

	if err := db.Set(ctx, "1", original); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	retrieved, err := db.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Strings["Email"] != original.Strings["Email"] {
		t.Errorf("Email mismatch: got %q, want %q", retrieved.Strings["Email"], original.Strings["Email"])
	}
	if retrieved.Strings["Name"] != original.Strings["Name"] {
		t.Errorf("Name mismatch: got %q, want %q", retrieved.Strings["Name"], original.Strings["Name"])
	}
}

func TestDatabase_Query(t *testing.T) {
	resetTable(t)
	ctx := context.Background()

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](testFactory, "id", "test_users", spec)

	// Insert test data
	_, _ = testDB.Exec(`
		INSERT INTO test_users (email, name, age) VALUES
		('alice@example.com', 'Alice', 25),
		('bob@example.com', 'Bob', 30),
		('carol@example.com', 'Carol', 35)
	`)

	atoms, err := db.Query(ctx, "query", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(atoms) != 3 {
		t.Errorf("expected 3 atoms, got %d", len(atoms))
	}

	// Verify atom contents
	names := make(map[string]bool)
	for _, a := range atoms {
		names[a.Strings["Name"]] = true
	}
	if !names["Alice"] || !names["Bob"] || !names["Carol"] {
		t.Errorf("expected Alice, Bob, Carol in results, got %v", names)
	}
}

func TestDatabase_Select(t *testing.T) {
	resetTable(t)
	ctx := context.Background()

	atomizer, _ := atom.Use[TestUser]()
	spec := atomizer.Spec()
	db := New[TestUser](testFactory, "id", "test_users", spec)

	_, _ = testDB.Exec(`INSERT INTO test_users (email, name, age) VALUES ('select@example.com', 'Select User', 45)`)

	// Add a custom select capability
	err := testFactory.AddSelect(edamame.SelectCapability{
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

	a, err := db.Select(ctx, "by-email", map[string]any{"email": "select@example.com"})
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if a.Strings["Name"] != "Select User" {
		t.Errorf("expected name 'Select User', got %q", a.Strings["Name"])
	}
}
