package flexpg

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"git.kanosolution.net/kano/dbflex"

	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	_ "github.com/lib/pq"
	"github.com/sebarcode/codekit"
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
	sqlconnstring := fmt.Sprintf("%s/%s", c.Host, c.Database)
	if c.User != "" {
		sqlconnstring = fmt.Sprintf("%s:%s@%s", c.User, c.Password, sqlconnstring)
	}
	sqlconnstring = "postgres://" + sqlconnstring
	configs := strings.Join(func() []string {
		var out []string
		for k, v := range c.Config {
			out = append(out, fmt.Sprintf("%s=%s", k, v))
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
	var e error
	cmdTxt := ""
	if !c.HasTable(name) {
		if cmdTxt, e = createCommandForCreateTable(name, keys, obj); e != nil {
			return e
		}
	} else {
		if cmdTxt, e = createCommandForUpdatingTable(c, name, obj); e != nil {
			return e
		}
		if cmdTxt == "" {
			return nil
		}
	}

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

func createCommandForCreateTable(name string, keys []string, obj interface{}) (string, error) {
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
			alias := f.Tag.Get(codekit.TagName())
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
				return "", fmt.Errorf("field %s has unmapped pg data type. %s", fieldName, fieldType)
			}
			options := []string{}
			if codekit.HasMember(keys, originalFieldName) || codekit.HasMember(keys, fieldName) {
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
		return "", errors.New("object should be a struct")
	}
	return fmt.Sprintf(tableCreateCommand, name, strings.Join(fields, ", ")), nil
}

func createCommandForUpdatingTable(c dbflex.IConnection, name string, obj interface{}) (string, error) {
	// get fields
	name = strings.ToLower(name)
	tableFields := []codekit.M{}
	sql := "select column_name,udt_name,is_nullable isnull from information_schema.columns where table_name='" + name + "'"
	e := c.Cursor(dbflex.SQL(sql), nil).Fetchs(&tableFields, 0).Close()
	if e != nil {
		return "", errors.New("unable to get table meta. " + e.Error())
	}
	//fmt.Println(tableFields)

	// convert fields to map to ease comparison
	mfs := make(map[string]codekit.M, len(tableFields))
	for _, f := range tableFields {
		mfs[strings.ToLower(f.GetString("column_name"))] = f
	}

	tableUpdateCommand := "ALTER TABLE %s %s;"
	fields := []string{}
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return "", errors.New("object should be a struct")
	}

	t := v.Type()
	fnum := t.NumField()
	hasChange := false
	for i := 0; i < fnum; i++ {
		f := t.Field(i)
		fieldName := f.Name
		alias := f.Tag.Get(codekit.TagName())
		if alias == "-" {
			continue
		}
		if alias != "" {
			fieldName = alias
		}
		fieldType := f.Type.String()

		// check if field already exist
		old, exist := mfs[strings.ToLower(fieldName)]

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
			return "", fmt.Errorf("field %s has unmapped pg data type. %s", fieldName, fieldType)
		}

		if exist {
			if fieldType != strings.ToLower(old.GetString("udt_name")) {
				hasChange = true
				fields = append(fields, fmt.Sprintf("alter %s type %s", strings.ToLower(fieldName), fieldType))
			}
		} else {
			hasChange = true
			notnull := "NOT NULL DEFAULT "
			switch fieldType {
			case "text":
				notnull += "''"
			case "integer", "numeric (32,8)", "numeric (64,8)":
				notnull += "0"
			case "boolean":
				notnull += " 'F'"
			case "timestamptz":
				notnull += "to_timestamp(0)"
			}
			fields = append(fields, fmt.Sprintf("add %s %s %s", strings.ToLower(fieldName), fieldType, notnull))
		}
	}

	if !hasChange {
		return "", nil
	}

	return fmt.Sprintf(tableUpdateCommand, name, strings.Join(fields, ",\n")), nil
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

// trigger versioning
