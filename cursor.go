package flexpg

import (
	"errors"
	"time"

	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	"github.com/sebarcode/codekit"
)

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	rdbms.Cursor
}

func (c *Cursor) CastValue(value interface{}, typeName string) (interface{}, error) {
	var d interface{}
	var err error

	func() {
		if r := recover(); r != nil {
			err = errors.New(r.(string))
		}

		switch value := value.(type) {
		case int:
			d = value

		case int8:
			d = int(value)

		case int16:
			d = int(value)

		case int32:
			d = int(value)

		case int64:
			d = int(value)

		case float32:
			d = value

		case float64:
			d = value

		case time.Time:
			d = value

		case *time.Time:
			d = value

		case bool:
			d = value

		case string:
			d = value

		default:
			str := string(value.([]byte))
			switch typeName {
			case "codekit.M":
				if str == "null" {
					d = nil
				} else {
					m := codekit.M{}
					err = codekit.UnjsonFromString(str, &m)
					d = m
				}

			default:
				d = string(value.([]byte))
			}
		}
	}()

	return d, err
}
