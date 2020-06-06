package flexpg

import (
	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
)

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	rdbms.Cursor
}
