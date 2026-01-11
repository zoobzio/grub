package mssql

import (
	"database/sql"
	"testing"
)

func TestDriverRegistered(t *testing.T) {
	drivers := sql.Drivers()
	found := false
	for _, d := range drivers {
		if d == "sqlserver" || d == "mssql" {
			found = true
			break
		}
	}
	if !found {
		t.Error("sqlserver driver not registered")
	}
}

func TestRegister(t *testing.T) {
	// Register is a no-op; verify it doesn't panic.
	Register()
}
