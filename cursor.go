package flexpg

import (
	"database/sql"
	"fmt"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	"github.com/eaciit/toolkit"
)

var timeFormat string

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	dbflex.CursorBase
	fetcher *sql.Rows
	//dest      []interface{}
	columns   []string
	types     []string
	values    []interface{}
	valuesPtr []interface{}

	_this dbflex.ICursor

	query      Query
	isPrepared bool
}

func (c *Cursor) Reset() error {
	c.fetcher = nil
	c.isPrepared = false
	return nil
}

func (c *Cursor) SetFetcher(r *sql.Rows) error {
	c.fetcher = r
	err := c.PrepareForScan()
	if err != nil {
		return err
	}

	return nil
}

func (c *Cursor) SetThis(ic dbflex.ICursor) dbflex.ICursor {
	c._this = ic
	return ic
}

func (c *Cursor) this() dbflex.ICursor {
	return c._this
}

func (c *Cursor) ColumnNames() []string {
	return c.columns
}

func (c *Cursor) SetColumnNames(names []string) {
	c.columns = names
}

func (c *Cursor) ColumnTypes() []string {
	return c.types
}

func (c *Cursor) SetColumnTypes(types []string) {
	c.types = types
}

func (c *Cursor) PrepareForScan() error {
	if e := c.Error(); e != nil {
		return e
	}

	c.values = []interface{}{}
	c.valuesPtr = []interface{}{}
	if c.fetcher == nil {
		return fmt.Errorf("scan failed, fetcher is not available")
	}

	names, err := c.fetcher.Columns()
	if err != nil {
		return fmt.Errorf("scan failed. %s", err.Error())
	}
	if len(names) == 0 {
		return fmt.Errorf("scan failed, no columns metadata is available")
	}

	ctypes, err := c.fetcher.ColumnTypes()
	if err != nil {
		return fmt.Errorf("scan failed. %s", err.Error())
	}

	sqlTypes := []string{}
	values := []interface{}{}
	ptrs := []interface{}{}
	for _, ct := range ctypes {
		fieldtype := Mapper.PrepareScanValue(ct)
		if fieldtype != "" {
			sqlTypes = append(sqlTypes, fieldtype)
		} else {
			sqlTypes = append(sqlTypes, "")
		}
		values = append(values, []byte{})
	}

	for idx := range values {
		ptrs = append(ptrs, &values[idx])
	}

	c.values = values
	c.valuesPtr = ptrs
	c.columns = names
	c.types = sqlTypes

	c.isPrepared = true
	return nil
}

func (c *Cursor) Scan() error {
	if !c.isPrepared {
		if err := c.PrepareForScan(); err != nil {
			return err
		}
	}

	if c.Error() != nil {
		return c.Error()
	}

	if c.fetcher == nil {
		return toolkit.Error("cursor is not valid, no fetcher object specified")
	}

	if !c.fetcher.Next() {
		return dbflex.EOF
	}

	return c.fetcher.Scan(c.valuesPtr...)
}

func (c *Cursor) Values() []interface{} {
	return c.values
}

func (c *Cursor) SetValues(v []interface{}) {
	c.values = v
}

func (c *Cursor) ValuesPtr() []interface{} {
	return c.valuesPtr
}

func (c *Cursor) SetValuesPtr(ptrs []interface{}) {
	c.valuesPtr = ptrs
}

func (c *Cursor) Serialize(dest interface{}) error {
	var err error
	m := toolkit.M{}
	toolkit.Serde(dest, &m, "")

	columnNames := c.ColumnNames()
	sqlTypes := c.ColumnTypes()
	for idx, value := range c.Values() {
		name := columnNames[idx]
		fieldtype := sqlTypes[idx]

		if found := Mapper.ScanValue(&m, fieldtype, name, value); !found {
			// default is string or []byte if can't be marshal
			if v, ok := value.([]byte); ok {
				m.Set(name, string(v))
			} else {
				m.Set(name, value)
			}
		}

		// v, ok := value.([]byte)
		// if ok {
		// switch ft {
		// case "int":
		// 	m.Set(name, toolkit.ToInt(string(v), toolkit.RoundingAuto))

		// case "float64":
		// 	m.Set(name, toolkit.ToFloat64(string(v), 4, toolkit.RoundingAuto))

		// case "time.Time":
		// 	if dt, err := time.Parse(time.RFC3339, string(v)); err == nil {
		// 		m.Set(name, dt)
		// 	} else {
		// 		dt = toolkit.String2Date(string(v), TimeFormat())
		// 		m.Set(name, dt)
		// 	}
		// default:
		// 	m.Set(name, string(v))
		// }
		// } else {
		// 	m.Set(name, value)
		// }
	}

	err = toolkit.Serde(m, dest, "")
	if err != nil {
		return toolkit.Error(err.Error() + toolkit.Sprintf(" object: %s", toolkit.JsonString(m)))
	}
	return nil
}

func (c *Cursor) Fetch(obj interface{}) dbflex.ICursor {
	err := c.Scan()
	if err != nil {
		c.SetError(err)
		return c
	}

	if err = c.this().(rdbms.RdbmsCursor).Serialize(obj); err != nil {
		c.SetError(err)
		return c
	}

	return c
}

func (c *Cursor) Fetchs(obj interface{}, n int) dbflex.ICursor {
	var err error
	i := 0
	loop := true
	ms := []toolkit.M{}
	for loop {
		err = c.Scan()
		if err != nil {
			if err == dbflex.EOF {
				loop = false
				err = nil
			} else {
				c.SetError(err)
				return c
			}
		} else {
			mobj := toolkit.M{}
			err = c.this().(rdbms.RdbmsCursor).Serialize(&mobj)
			if err != nil {
				c.SetError(err)
				return c
			}
			ms = append(ms, mobj)
			i++
			if i >= n && n != 0 {
				//fmt.Println("data", i)
				loop = false
			}
		}
	}

	err = toolkit.Serde(ms, obj, "")
	if err != nil {
		c.SetError(err)
		return c
	}
	return c
}

func (c *Cursor) Close() error {
	var e error
	if c != nil {
		e = c.Error()
		if c.fetcher != nil {
			c.fetcher.Close()
		}
	}
	return e
}

func TimeFormat() string {
	if timeFormat == "" {
		timeFormat = "yyyy-MM-dd hh:mm:ss"
	}
	return timeFormat
}

func (c *Cursor) SetTimeFormat(f string) {
	timeFormat = f
}
