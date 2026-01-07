// Package postgres registers the PostgreSQL driver for use with grub.Database.
package postgres

import _ "github.com/lib/pq"

// Register is a no-op; importing this package registers the driver.
func Register() {}
