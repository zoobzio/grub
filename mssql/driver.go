// Package mssql registers the Microsoft SQL Server driver for use with grub.Database.
package mssql

import _ "github.com/microsoft/go-mssqldb"

// Register is a no-op; importing this package registers the driver.
func Register() {}
