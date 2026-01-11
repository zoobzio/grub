package mockdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

func TestNew(t *testing.T) {
	db, capture := New()
	if db == nil {
		t.Fatal("New returned nil db")
	}
	if capture == nil {
		t.Fatal("New returned nil capture")
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Errorf("db.PingContext failed: %v", err)
	}
}

func TestNewWithConfig(t *testing.T) {
	db, capture, config := NewWithConfig()
	if db == nil {
		t.Fatal("NewWithConfig returned nil db")
	}
	if capture == nil {
		t.Fatal("NewWithConfig returned nil capture")
	}
	if config == nil {
		t.Fatal("NewWithConfig returned nil config")
	}
}

func TestNew_ResetsCapture(t *testing.T) {
	db1, capture1 := New()
	ctx := context.Background()

	// Execute a query to populate capture
	_, _ = db1.ExecContext(ctx, "INSERT INTO test VALUES (?)", 1)

	if len(capture1.Queries) == 0 {
		t.Fatal("expected query to be captured")
	}

	// New should reset the capture
	_, capture2 := New()

	if len(capture2.Queries) != 0 {
		t.Errorf("expected capture to be reset, got %d queries", len(capture2.Queries))
	}
}

func TestNew_ResetsConfig(t *testing.T) {
	_, _, config := NewWithConfig()
	config.SetQueryErr(errors.New("test error"))
	config.SetExecErr(errors.New("exec error"))
	config.SetRowsAffected(99)

	// New should reset the config
	NewWithConfig()

	if config.getQueryErr() != nil {
		t.Error("expected QueryErr to be reset")
	}
	if config.getExecErr() != nil {
		t.Error("expected ExecErr to be reset")
	}
	if config.getRowsAffected() != 1 {
		t.Errorf("expected RowsAffected to be default 1, got %d", config.getRowsAffected())
	}
}

func TestDriver_Open(t *testing.T) {
	d := &Driver{}
	conn, err := d.Open("")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if conn == nil {
		t.Fatal("Open returned nil connection")
	}
}

func TestCapture_AddAndLast(t *testing.T) {
	c := &Capture{}

	// Empty capture
	_, ok := c.Last()
	if ok {
		t.Error("expected Last to return false for empty capture")
	}

	// Add queries
	c.add("SELECT * FROM users", []any{})
	c.add("INSERT INTO users VALUES (?)", []any{1})

	last, ok := c.Last()
	if !ok {
		t.Fatal("expected Last to return true")
	}
	if last.Query != "INSERT INTO users VALUES (?)" {
		t.Errorf("unexpected query: %s", last.Query)
	}
	if len(last.Args) != 1 || last.Args[0] != 1 {
		t.Errorf("unexpected args: %v", last.Args)
	}
}

func TestCapture_Reset(t *testing.T) {
	c := &Capture{}
	c.add("SELECT 1", nil)
	c.add("SELECT 2", nil)

	if len(c.Queries) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(c.Queries))
	}

	c.Reset()

	if len(c.Queries) != 0 {
		t.Errorf("expected 0 queries after reset, got %d", len(c.Queries))
	}

	_, ok := c.Last()
	if ok {
		t.Error("expected Last to return false after reset")
	}
}

func TestConfig_SetQueryErr(t *testing.T) {
	c := &Config{}
	testErr := errors.New("query error")

	c.SetQueryErr(testErr)
	if !errors.Is(c.getQueryErr(), testErr) {
		t.Error("SetQueryErr did not set error")
	}

	c.SetQueryErr(nil)
	if c.getQueryErr() != nil {
		t.Error("SetQueryErr did not clear error")
	}
}

func TestConfig_SetExecErr(t *testing.T) {
	c := &Config{}
	testErr := errors.New("exec error")

	c.SetExecErr(testErr)
	if !errors.Is(c.getExecErr(), testErr) {
		t.Error("SetExecErr did not set error")
	}

	c.SetExecErr(nil)
	if c.getExecErr() != nil {
		t.Error("SetExecErr did not clear error")
	}
}

func TestConfig_SetRowsAffected(t *testing.T) {
	c := &Config{}

	// Default value (not set)
	if c.getRowsAffected() != 1 {
		t.Errorf("expected default rows affected 1, got %d", c.getRowsAffected())
	}

	c.SetRowsAffected(5)
	if c.getRowsAffected() != 5 {
		t.Errorf("expected rows affected 5, got %d", c.getRowsAffected())
	}

	// Zero should be respected once explicitly set
	c.SetRowsAffected(0)
	if c.getRowsAffected() != 0 {
		t.Errorf("expected rows affected 0, got %d", c.getRowsAffected())
	}
}

func TestConfig_Reset(t *testing.T) {
	c := &Config{}
	c.SetQueryErr(errors.New("error"))
	c.SetExecErr(errors.New("error"))
	c.SetRowsAffected(99)

	c.Reset()

	if c.getQueryErr() != nil {
		t.Error("Reset did not clear QueryErr")
	}
	if c.getExecErr() != nil {
		t.Error("Reset did not clear ExecErr")
	}
	if c.getRowsAffected() != 1 {
		t.Errorf("Reset did not restore default RowsAffected, got %d", c.getRowsAffected())
	}
}

func TestConn_QueryContext(t *testing.T) {
	capture := &Capture{}
	config := &Config{}
	conn := &Conn{capture: capture, config: config}

	args := []driver.NamedValue{
		{Ordinal: 1, Value: "test"},
		{Ordinal: 2, Value: 42},
	}

	rows, err := conn.QueryContext(context.Background(), "SELECT * FROM users WHERE name = ? AND age = ?", args)
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	if rows == nil {
		t.Fatal("QueryContext returned nil rows")
	}

	last, ok := capture.Last()
	if !ok {
		t.Fatal("query not captured")
	}
	if last.Query != "SELECT * FROM users WHERE name = ? AND age = ?" {
		t.Errorf("unexpected query: %s", last.Query)
	}
	if len(last.Args) != 2 || last.Args[0] != "test" || last.Args[1] != 42 {
		t.Errorf("unexpected args: %v", last.Args)
	}
}

func TestConn_QueryContext_Error(t *testing.T) {
	capture := &Capture{}
	config := &Config{}
	conn := &Conn{capture: capture, config: config}

	testErr := errors.New("query failed")
	config.SetQueryErr(testErr)

	rows, err := conn.QueryContext(context.Background(), "SELECT 1", nil)
	if rows != nil {
		_ = rows.Close()
	}
	if !errors.Is(err, testErr) {
		t.Errorf("expected query error, got: %v", err)
	}

	// Query should still be captured even on error
	if len(capture.Queries) != 1 {
		t.Errorf("expected query to be captured even on error")
	}
}

func TestConn_ExecContext(t *testing.T) {
	capture := &Capture{}
	config := &Config{}
	conn := &Conn{capture: capture, config: config}

	args := []driver.NamedValue{
		{Ordinal: 1, Value: "test@example.com"},
	}

	result, err := conn.ExecContext(context.Background(), "INSERT INTO users (email) VALUES (?)", args)
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	lastID, _ := result.LastInsertId()
	if lastID != 1 {
		t.Errorf("expected LastInsertId 1, got %d", lastID)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != 1 {
		t.Errorf("expected RowsAffected 1, got %d", rowsAffected)
	}

	last, ok := capture.Last()
	if !ok {
		t.Fatal("query not captured")
	}
	if last.Query != "INSERT INTO users (email) VALUES (?)" {
		t.Errorf("unexpected query: %s", last.Query)
	}
}

func TestConn_ExecContext_Error(t *testing.T) {
	capture := &Capture{}
	config := &Config{}
	conn := &Conn{capture: capture, config: config}

	testErr := errors.New("exec failed")
	config.SetExecErr(testErr)

	_, err := conn.ExecContext(context.Background(), "DELETE FROM users", nil)
	if !errors.Is(err, testErr) {
		t.Errorf("expected exec error, got: %v", err)
	}
}

func TestConn_ExecContext_RowsAffected(t *testing.T) {
	capture := &Capture{}
	config := &Config{}
	conn := &Conn{capture: capture, config: config}

	config.SetRowsAffected(5)

	result, err := conn.ExecContext(context.Background(), "UPDATE users SET active = 1", nil)
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != 5 {
		t.Errorf("expected RowsAffected 5, got %d", rowsAffected)
	}
}

func TestConn_Prepare(t *testing.T) {
	capture := &Capture{}
	config := &Config{}
	conn := &Conn{capture: capture, config: config}

	stmt, err := conn.Prepare("SELECT * FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Prepare returned nil statement")
	}
}

func TestConn_Close(t *testing.T) {
	conn := &Conn{}
	if err := conn.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

func TestConn_Begin(t *testing.T) {
	conn := &Conn{}
	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if tx == nil {
		t.Fatal("Begin returned nil transaction")
	}
}

func TestConn_Capture(t *testing.T) {
	capture := &Capture{}
	conn := &Conn{capture: capture}

	if conn.Capture() != capture {
		t.Error("Capture() did not return the expected capture")
	}
}

func TestStmt_Query(t *testing.T) {
	capture := &Capture{}
	stmt := &Stmt{query: "SELECT * FROM users WHERE id = ?", capture: capture}

	rows, err := stmt.Query([]driver.Value{123})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if rows == nil {
		t.Fatal("Query returned nil rows")
	}

	last, ok := capture.Last()
	if !ok {
		t.Fatal("query not captured")
	}
	if last.Query != "SELECT * FROM users WHERE id = ?" {
		t.Errorf("unexpected query: %s", last.Query)
	}
	if len(last.Args) != 1 || last.Args[0] != 123 {
		t.Errorf("unexpected args: %v", last.Args)
	}
}

func TestStmt_Exec(t *testing.T) {
	capture := &Capture{}
	stmt := &Stmt{query: "DELETE FROM users WHERE id = ?", capture: capture}

	result, err := stmt.Exec([]driver.Value{456})
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}
	if result == nil {
		t.Fatal("Exec returned nil result")
	}

	last, ok := capture.Last()
	if !ok {
		t.Fatal("query not captured")
	}
	if last.Query != "DELETE FROM users WHERE id = ?" {
		t.Errorf("unexpected query: %s", last.Query)
	}
}

func TestStmt_NumInput(t *testing.T) {
	stmt := &Stmt{}
	if stmt.NumInput() != -1 {
		t.Errorf("expected NumInput -1, got %d", stmt.NumInput())
	}
}

func TestStmt_Close(t *testing.T) {
	stmt := &Stmt{}
	if err := stmt.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

func TestTx_Commit(t *testing.T) {
	tx := &Tx{}
	if err := tx.Commit(); err != nil {
		t.Errorf("Commit returned error: %v", err)
	}
}

func TestTx_Rollback(t *testing.T) {
	tx := &Tx{}
	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback returned error: %v", err)
	}
}

func TestResult_LastInsertId(t *testing.T) {
	r := &Result{}
	id, err := r.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId returned error: %v", err)
	}
	if id != 1 {
		t.Errorf("expected LastInsertId 1, got %d", id)
	}
}

func TestResult_RowsAffected(t *testing.T) {
	r := &Result{rowsAffected: 10}
	n, err := r.RowsAffected()
	if err != nil {
		t.Errorf("RowsAffected returned error: %v", err)
	}
	if n != 10 {
		t.Errorf("expected RowsAffected 10, got %d", n)
	}
}

func TestRows_Columns(t *testing.T) {
	r := &Rows{}
	cols := r.Columns()
	if len(cols) != 0 {
		t.Errorf("expected empty columns, got %v", cols)
	}
}

func TestRows_Close(t *testing.T) {
	r := &Rows{}
	if r.closed {
		t.Error("rows should not be closed initially")
	}

	if err := r.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	if !r.closed {
		t.Error("rows should be closed after Close()")
	}
}

func TestRows_Next(t *testing.T) {
	r := &Rows{}
	err := r.Next(nil)
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestNamedValuesToAny(t *testing.T) {
	nvs := []driver.NamedValue{
		{Ordinal: 1, Value: "string"},
		{Ordinal: 2, Value: 42},
		{Ordinal: 3, Value: true},
	}

	result := namedValuesToAny(nvs)

	if len(result) != 3 {
		t.Fatalf("expected 3 values, got %d", len(result))
	}
	if result[0] != "string" {
		t.Errorf("expected 'string', got %v", result[0])
	}
	if result[1] != 42 {
		t.Errorf("expected 42, got %v", result[1])
	}
	if result[2] != true {
		t.Errorf("expected true, got %v", result[2])
	}
}

func TestValuesToAny(t *testing.T) {
	vs := []driver.Value{"string", 42, true}

	result := valuesToAny(vs)

	if len(result) != 3 {
		t.Fatalf("expected 3 values, got %d", len(result))
	}
	if result[0] != "string" {
		t.Errorf("expected 'string', got %v", result[0])
	}
	if result[1] != 42 {
		t.Errorf("expected 42, got %v", result[1])
	}
	if result[2] != true {
		t.Errorf("expected true, got %v", result[2])
	}
}
