package grub

import (
	"context"
	"strings"
	"testing"

	astqlsqlite "github.com/zoobzio/astql/sqlite"
	"github.com/zoobzio/edamame"
	"github.com/zoobzio/grub/internal/mockdb"
	"github.com/zoobzio/sentinel"
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

var testDBRenderer = astqlsqlite.New()

func TestNewDatabase(t *testing.T) {
	mockDB, _ := mockdb.New()
	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}
	if db == nil {
		t.Fatal("NewDatabase returned nil")
	}
	if db.executor == nil {
		t.Error("executor not set")
	}
	if db.keyCol != "id" {
		t.Errorf("keyCol mismatch: got %q", db.keyCol)
	}
	if db.tableName != "test_users" {
		t.Errorf("tableName mismatch: got %q", db.tableName)
	}
}

func TestDatabase_Get(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Call Get - it will fail to find data but we can check the SQL
	_, _ = db.Get(ctx, "123")

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	// Verify SELECT query structure
	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"test_users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"id"`) {
		t.Errorf("expected id column in WHERE clause, got: %s", query.Query)
	}
}

func TestDatabase_Set(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	user := &TestDBUser{
		ID:    1,
		Email: "test@example.com",
		Name:  "Test User",
		Age:   intPtr(30),
	}

	_ = db.Set(ctx, "1", user)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	// Verify INSERT/UPSERT query structure
	if !strings.Contains(query.Query, "INSERT") {
		t.Errorf("expected INSERT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"test_users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, "ON CONFLICT") {
		t.Errorf("expected ON CONFLICT clause for upsert, got: %s", query.Query)
	}
}

func TestDatabase_Delete(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	_ = db.Delete(ctx, "123")

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	// Verify DELETE query structure
	if !strings.Contains(query.Query, "DELETE") {
		t.Errorf("expected DELETE query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"test_users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"id"`) {
		t.Errorf("expected id column in WHERE clause, got: %s", query.Query)
	}
}

func TestDatabase_Exists(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	_, _ = db.Exists(ctx, "123")

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	// Verify SELECT with LIMIT query structure
	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, "LIMIT") {
		t.Errorf("expected LIMIT clause, got: %s", query.Query)
	}
}

func TestDatabase_Executor(t *testing.T) {
	mockDB, _ := mockdb.New()
	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	executor := db.Executor()
	if executor == nil {
		t.Error("Executor returned nil")
	}
}

func TestDatabase_Query(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Use default QueryAll statement
	_, _ = db.Query(ctx, QueryAll, nil)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"test_users"`) {
		t.Errorf("expected table name in query, got: %s", query.Query)
	}
}

func TestDatabase_QueryWithStatement(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	stmt := edamame.NewQueryStatement("by-min-age", "Users with age >= min_age", edamame.QuerySpec{
		Where: []edamame.ConditionSpec{
			{Field: "age", Operator: ">=", Param: "min_age"},
		},
		OrderBy: []edamame.OrderBySpec{
			{Field: "age", Direction: "asc"},
		},
	})

	_, _ = db.Query(ctx, stmt, map[string]any{"min_age": 30})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, `"age"`) {
		t.Errorf("expected age column in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, ">=") {
		t.Errorf("expected >= operator in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, "ORDER BY") {
		t.Errorf("expected ORDER BY clause, got: %s", query.Query)
	}
}

func TestDatabase_Select(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	stmt := edamame.NewSelectStatement("by-email", "Find user by email", edamame.SelectSpec{
		Where: []edamame.ConditionSpec{
			{Field: "email", Operator: "=", Param: "email"},
		},
	})

	_, _ = db.Select(ctx, stmt, map[string]any{"email": "test@example.com"})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SELECT") {
		t.Errorf("expected SELECT query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"email"`) {
		t.Errorf("expected email column in query, got: %s", query.Query)
	}
}

func TestDatabase_Update(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	stmt := edamame.NewUpdateStatement("rename-by-email", "Update user name by email", edamame.UpdateSpec{
		Set: map[string]string{
			"name": "new_name",
		},
		Where: []edamame.ConditionSpec{
			{Field: "email", Operator: "=", Param: "email"},
		},
	})

	_, _ = db.Update(ctx, stmt, map[string]any{
		"email":    "test@example.com",
		"new_name": "Updated",
	})

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "UPDATE") {
		t.Errorf("expected UPDATE query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"name"`) {
		t.Errorf("expected name column in SET clause, got: %s", query.Query)
	}
}

func TestDatabase_Aggregate(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	// Default count aggregate
	_, _ = db.Aggregate(ctx, CountAll, nil)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "COUNT") {
		t.Errorf("expected COUNT in query, got: %s", query.Query)
	}
}

func TestDatabase_AggregateSum(t *testing.T) {
	mockDB, capture := mockdb.New()
	ctx := context.Background()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	stmt := edamame.NewAggregateStatement("sum-age", "Sum of all ages", edamame.AggSum, edamame.AggregateSpec{
		Field: "age",
	})

	_, _ = db.Aggregate(ctx, stmt, nil)

	query, ok := capture.Last()
	if !ok {
		t.Fatal("no query captured")
	}

	if !strings.Contains(query.Query, "SUM") {
		t.Errorf("expected SUM in query, got: %s", query.Query)
	}
	if !strings.Contains(query.Query, `"age"`) {
		t.Errorf("expected age column in query, got: %s", query.Query)
	}
}

func TestDatabase_Atomic(t *testing.T) {
	mockDB, _ := mockdb.New()

	db, err := NewDatabase[TestDBUser](mockDB, "test_users", "id", testDBRenderer)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	atomic := db.Atomic()
	if atomic == nil {
		t.Fatal("Atomic returned nil")
	}

	// Verify it returns the same instance
	atomic2 := db.Atomic()
	if atomic != atomic2 {
		t.Error("Atomic should return cached instance")
	}
}

func intPtr(i int) *int {
	return &i
}
