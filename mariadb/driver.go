// Package mariadb registers the MariaDB driver for use with grub.Database.
package mariadb

import _ "github.com/go-sql-driver/mysql"

// Register is a no-op; importing this package registers the driver.
func Register() {}
