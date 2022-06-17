package flexpg

import (
	"errors"
	"time"

	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
)

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	rdbms.Cursor
}

func (c *Cursor) castValue(value interface{}, typeName string) (interface{}, error) {
	var d interface{}
	var err error

	func() {
		if r := recover(); r != nil {
			err = errors.New(r.(string))
		}

		switch value.(type) {
		case int, int8, int16, int32, int64:
			d = int(value.(int64))

		case float32:
			d = value.(float32)

		case float64:
			d = value.(float64)

		case time.Time:
			d = value.(time.Time)

		case bool:
			d = value.(bool)

		case string:
			d = value.(string)

		default:
			d = string(value.([]byte))
		}
	}()

	return d, err
}
