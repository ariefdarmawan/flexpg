package flexpg

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"git.kanosolution.net/kano/dbflex"
	"github.com/eaciit/toolkit"

	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	_ "github.com/lib/pq"
)

// Connection implementation of dbflex.IConnection
type Connection struct {
	rdbms.Connection
	db *sql.DB
	tx *sql.Tx
}

func init() {
	dbflex.RegisterDriver("postgres", func(si *dbflex.ServerInfo) dbflex.IConnection {
		c := new(Connection)
		c.SetThis(c)
		c.ServerInfo = *si
		return c
	})
}

// Connect to database instance
func (c *Connection) Connect() error {
	sqlconnstring := toolkit.Sprintf("%s/%s", c.Host, c.Database)
	if c.User != "" {
		sqlconnstring = toolkit.Sprintf("%s:%s@%s", c.User, c.Password, sqlconnstring)
	}
	sqlconnstring = "postgres://" + sqlconnstring
	configs := strings.Join(func() []string {
		var out []string
		for k, v := range c.Config {
			out = append(out, toolkit.Sprintf("%s=%s", k, v))
		}
		return out
	}(), "&")
	if configs != "" {
		sqlconnstring = sqlconnstring + "?" + configs
	}
	db, err := sql.Open("postgres", sqlconnstring)
	c.db = db
	return err
}

func (c *Connection) State() string {
	if c.db != nil {
		return dbflex.StateConnected
	}
	return dbflex.StateUnknown
}

// Close database connection
func (c *Connection) Close() {
	if c.db != nil {
		c.db.Close()
	}
}

// NewQuery generates new query object to perform query action
func (c *Connection) NewQuery() dbflex.IQuery {
	q := new(Query)
	q.SetThis(q)
	q.conn = c
	return q
}

func (c *Connection) DropTable(name string) error {
	cmd := "DROP TABLE " + name
	var e error
	if c.IsTx() {
		_, e = c.tx.Exec(cmd)
	} else {
		_, e = c.db.Exec(cmd)
	}
	return e
}

func (c *Connection) EnsureTable(name string, keys []string, obj interface{}) error {
	tableCreateCommand := "CREATE TABLE %s (%s);"

	fields := []string{}
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Struct {
		t := v.Type()
		fnum := t.NumField()
		for i := 0; i < fnum; i++ {
			f := t.Field(i)
			tag := f.Tag
			fieldName := f.Name
			alias := f.Tag.Get(toolkit.TagName())
			originalFieldName := fieldName
			if alias == "-" {
				continue
			}
			if alias != "" {
				fieldName = alias
			}
			fieldType := f.Type.String()
			if fieldType == "string" {
				fieldType = "text"
			} else if fieldType != "interface{}" && strings.HasPrefix(fieldType, "int") {
				fieldType = "integer"
			} else if strings.Contains(fieldType, "time.Time") {
				fieldType = "timestamptz"
			} else if fieldType == "float32" {
				fieldType = "numeric (32,8)"
			} else if fieldType == "float64" {
				fieldType = "numeric (64,8)"
			} else if fieldType == "bool" {
				fieldType = "boolean"
			} else {
				return fmt.Errorf("field %s has unmapped pg data type. %s", fieldName, fieldType)
			}
			options := []string{}
			if toolkit.HasMember(keys, originalFieldName) || toolkit.HasMember(keys, fieldName) {
				options = append(options, "PRIMARY KEY")
			}
			if _, ok := tag.Lookup("required"); ok {
				options = append(options, "NOT NULL")
			}
			fields = append(fields, strings.Replace(fmt.Sprintf("%s %s %s",
				strings.ToLower(fieldName),
				fieldType,
				strings.Join(options, " ")),
				"  ", " ", -1))
		}
	} else {
		return errors.New("object should be a struct")
	}

	cmdTxt := fmt.Sprintf(tableCreateCommand, name, strings.Join(fields, ", "))

	var e error
	if c.IsTx() {
		_, e = c.tx.Exec(cmdTxt)
	} else {
		_, e = c.db.Exec(cmdTxt)
	}

	if e != nil {
		return fmt.Errorf("error: %s command: %s", e.Error(), cmdTxt)
	}
	return nil
}

func (c *Connection) BeginTx() error {
	if c.IsTx() {
		return errors.New("already in transaction mode. Please commit or rollback first")
	}
	tx, e := c.db.Begin()
	if e != nil {
		return e
	}
	c.tx = tx
	return nil
}

func (c *Connection) Commit() error {
	if !c.IsTx() {
		return fmt.Errorf("not is transaction mode")
	}
	if e := c.tx.Commit(); e != nil {
		return e
	}
	c.tx = nil
	return nil
}

func (c *Connection) RollBack() error {
	if !c.IsTx() {
		return fmt.Errorf("not is transaction mode")
	}
	if e := c.tx.Rollback(); e != nil {
		return e
	}
	c.tx = nil
	return nil
}

func (c *Connection) SupportTx() bool {
	return true
}

func (c *Connection) IsTx() bool {
	return c.tx != nil
}

func (c *Connection) Tx() *sql.Tx {
	return c.tx
}
