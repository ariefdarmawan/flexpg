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
		dbflex.Logger().Debugf("SQL Command: %v", cmdTxt)
		_, e = c.tx.Exec(cmdTxt)
	} else {
		dbflex.Logger().Debugf("SQL Command: %v", cmdTxt)
		_, e = c.db.Exec(cmdTxt)
	}

	if e != nil {
		return fmt.Errorf("error: %s command: %s", e.Error(), cmdTxt)
	}
	return nil
}

func createCommandForCreateTable(name string, keys []string, someobj interface{}) (string, error) {
	tableCreateCommand := "CREATE TABLE %s (%s);"
	fields := []string{}
	if e := toFieldForCreateTable(someobj, &keys, &fields); e != nil {
		return "", e
	}
	return fmt.Sprintf(tableCreateCommand, name, strings.Join(fields, ", ")), nil
}

func toFieldForCreateTable(obj interface{}, keys *[]string, fields *[]string) error {
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
			fieldTypeName := f.Type.String()
			fieldType := f.Type.Kind()
			if fieldType == reflect.Ptr {
				fieldType = f.Type.Elem().Kind()
			}
			alias := f.Tag.Get(toolkit.TagName())
			originalFieldName := fieldName
			if alias == "-" {
				continue
			}
			if alias != "" {
				fieldName = alias
			}
			dbType := Mapper.MigrationDBType(f)
			if dbType == "" {
				if fieldType != reflect.Struct {
					return fmt.Errorf("field %s has unmapped pg data type. %s", fieldName, fieldTypeName)
				}
				nf := v.Field(i).Interface()
				if e := toFieldForCreateTable(nf, keys, fields); e != nil {
					return e
				}
				continue
			}
			options := []string{}
			if toolkit.HasMember(keys, originalFieldName) || toolkit.HasMember(keys, fieldName) {
				options = append(options, "PRIMARY KEY")
			}
			if _, ok := tag.Lookup("required"); ok {
				notnull := "NOT NULL"
				if defValue, found := Mapper.MigrationDBTypeDefaultNotNull(f, dbType); found {
					notnull += " DEFAULT " + defValue
				}
				options = append(options, notnull)
			} else {
				if defValue, found := Mapper.MigrationDBTypeDefaultNotNull(f, dbType); found {
					options = append(options, " DEFAULT "+defValue)
				}
			}
			*fields = append(*fields, strings.Replace(fmt.Sprintf("%s %s %s",
				strings.ToLower(fieldName),
				dbType,
				strings.Join(options, " ")),
				"  ", " ", -1))
		}
	} else {
		return errors.New("object should be a struct")
	}
	return nil
}

func createCommandForUpdatingTable(c dbflex.IConnection, name string, someobj interface{}) (string, error) {
	// get fields
	tableFields := []toolkit.M{}
	sql := "select column_name,udt_name,is_nullable isnull from information_schema.columns where table_name='" + name + "'"
	e := c.Cursor(dbflex.SQL(sql), nil).Fetchs(&tableFields, 0).Close()
	if e != nil {
		return "", errors.New("unable to get table meta. " + e.Error())
	}

	// convert fields to map to ease comparison
	mfs := make(map[string]toolkit.M, len(tableFields))
	for _, f := range tableFields {
		mfs[strings.ToLower(f.GetString("column_name"))] = f
	}

	tableUpdateCommand := "ALTER TABLE %s %s;"
	hasChange := false
	fields := []string{}

	if e := toFieldForUpdatingTable(someobj, mfs, &hasChange, &fields); e != nil {
		return "", e
	}

	if !hasChange {
		return "", nil
	}

	return fmt.Sprintf(tableUpdateCommand, name, strings.Join(fields, ",\n")), nil
}

func toFieldForUpdatingTable(obj interface{}, mfs map[string]toolkit.M, hasChange *bool, fields *[]string) error {
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return errors.New("object should be a struct")
	}

	t := v.Type()
	fnum := t.NumField()
	for i := 0; i < fnum; i++ {
		f := t.Field(i)
		fieldName := f.Name
		fieldTypeName := f.Type.String()
		fieldType := f.Type.Kind()
		alias := f.Tag.Get(toolkit.TagName())
		if alias == "-" {
			continue
		}
		if alias != "" {
			fieldName = alias
		}

		// check if field already exist
		old, exist := mfs[strings.ToLower(fieldName)]

		dbType := Mapper.MigrationDBType(f)
		if dbType == "" {
			if fieldType != reflect.Struct { // maybe nested
				return fmt.Errorf("field %s has unmapped pg data type. %s", fieldName, fieldTypeName)
			}
			nf := v.Field(i).Interface()
			if e := toFieldForUpdatingTable(nf, mfs, hasChange, fields); e != nil {
				return e
			}
			continue
		}

		if exist {
			// Checking old datatype != new datatype with some alias (because some datatype can be compare as same)
			if dbType != strings.ToLower(old.GetString("udt_name")) {
				*hasChange = true
				*fields = append(*fields, fmt.Sprintf("alter %s type %s", strings.ToLower(fieldName), dbType))
			}
		} else {
			*hasChange = true
			options := []string{}
			if _, ok := f.Tag.Lookup("required"); ok {
				notnull := "NOT NULL"
				if defValue, found := Mapper.MigrationDBTypeDefaultNotNull(f, dbType); found {
					notnull += " DEFAULT " + defValue
				}
				options = append(options, notnull)
			} else {
				if defValue, found := Mapper.MigrationDBTypeDefaultNotNull(f, dbType); found {
					options = append(options, " DEFAULT "+defValue)
				}
			}
			*fields = append(*fields, fmt.Sprintf("add %s %s %s", strings.ToLower(fieldName), dbType, strings.Join(options, " ")))
		}
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
