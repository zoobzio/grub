// Package sqlite registers the SQLite driver for use with grub.Database.
package sqlite

import _ "modernc.org/sqlite"

// Register is a no-op; importing this package registers the driver.
func Register() {}
