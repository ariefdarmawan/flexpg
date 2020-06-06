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
	q.db = c.db
	return q
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
			fieldName := f.Name
			alias := f.Tag.Get(toolkit.TagName())
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
			if toolkit.HasMember(keys, fieldName) {
				fields = append(fields, fmt.Sprintf("%s %s PRIMARY KEY", strings.ToLower(fieldName), fieldType))
			} else {
				fields = append(fields, fmt.Sprintf("%s %s", strings.ToLower(fieldName), fieldType))
			}
		}
	} else {
		return errors.New("object should be a struct")
	}

	cmdTxt := fmt.Sprintf(tableCreateCommand, name, strings.Join(fields, ", "))
	_, e := c.db.Exec(cmdTxt)
	if e != nil {
		return fmt.Errorf("error: %s command: %s", e.Error(), cmdTxt)
	}
	return nil
}
