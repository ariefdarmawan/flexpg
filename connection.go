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

	txIsDisabled bool
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
	cmdTxts := []string{}
	if !c.HasTable(name) {
		if cmdTxt, e := createCommandForCreateTable(name, keys, obj); e != nil {
			return e
		} else {
			cmdTxts = append(cmdTxts, cmdTxt)
		}
	} else {
		if cmdTxts, e = createCommandForUpdatingTable(c, name, obj); e != nil {
			return e
		}
		if len(cmdTxts) == 0 {
			return nil
		}
	}

	for _, cmdTxt := range cmdTxts {
		dbflex.Logger().Info(cmdTxt)
		if c.IsTx() {
			_, e = c.tx.Exec(cmdTxt)
		} else {
			_, e = c.db.Exec(cmdTxt)
		}
		if e != nil {
			return fmt.Errorf("error: %s command: %s", e.Error(), cmdTxt)
		}
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
				fieldType = "varchar(255)"
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
				fieldType = "jsonb"
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

func createCommandForUpdatingTable(c dbflex.IConnection, name string, obj interface{}) ([]string, error) {
	res := []string{}

	// get fields
	name = strings.ToLower(name)
	tableFields := []codekit.M{}
	sql := "select column_name,udt_name,is_nullable as isnull, 0::bool as included from information_schema.columns where table_name='" + name + "' order by ordinal_position"
	e := c.Cursor(dbflex.SQL(sql), nil).Fetchs(&tableFields, 0).Close()
	if e != nil {
		return res, errors.New("unable to get table meta. " + e.Error())
	}

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
		return res, errors.New("object should be a struct")
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
		dbType := f.Tag.Get("DBType")
		fieldType := f.Type.String()

		// check if field already exist
		old, exist := mfs[strings.ToLower(fieldName)]

		if dbType == "" {
			if fieldType == "string" {
				fieldType = "varchar(255)"
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
				fieldType = "jsonb"
			}
		} else {
			fieldType = dbType
		}

		if exist {
			oldUdtName := strings.ToLower(old.GetString("udt_name"))
			if fieldType != oldUdtName {
				hasChange = true
				fields = append(fields, fmt.Sprintf("alter %s type %s", strings.ToLower(fieldName), fieldType))
			}
			old.Set("included", true)
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
			default:
				notnull += "''"
			}
			fields = append(fields, fmt.Sprintf("add %s %s null", strings.ToLower(fieldName), fieldType))
		}
	}

	if !hasChange {
		return res, nil
	}
	res = append(res, fmt.Sprintf(tableUpdateCommand, name, strings.Join(fields, ",\n")))

	for _, mf := range mfs {
		if !mf.GetBool("included") {
			res = append(res, fmt.Sprintf("alter table %s drop column %s", name, mf.GetString("column_name")))
		}
	}

	return res, nil
}

func (c *Connection) BeginTx() error {
	if c.IsTx() {
		return errors.New("already in transaction mode. Please commit or rollback first")
	}
	if c.txIsDisabled {
		return errors.New("tx is disabled")
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

func (c *Connection) DisableTx(disable bool) {
	c.txIsDisabled = disable
}

func (c *Connection) IsTx() bool {
	return c.tx != nil
}

func (c *Connection) Tx() *sql.Tx {
	return c.tx
}

// trigger versioning

func (c *Connection) EnsureIndex(tableName, idxName string, isUnique bool, fields ...string) error {
	indexName := strings.ToLower(fmt.Sprintf("%s_%s", tableName, idxName))

	res := []string{}
	record := codekit.M{}
	cmdGetIndexCount := fmt.Sprintf("SELECT count(*)::int as indexCount FROM pg_indexes WHERE indexname = '%s'", indexName)
	c.Cursor(dbflex.SQL(cmdGetIndexCount), nil).Fetch(&record).Error()
	if record.GetInt("indexcount") == 1 {
		res = append(res, fmt.Sprintf("drop index %s", indexName))
	}

	for idx, field := range fields {
		if strings.Index(field, ".") > 0 {
			fieldParts := strings.Split(field, ".")
			for fpIndex, fp := range fieldParts {
				if fpIndex > 0 {
					fieldParts[fpIndex] = fmt.Sprintf("'%s'", fp)
				}
			}
			fields[idx] = fmt.Sprintf("(%s)", strings.Join(fieldParts, "->>"))
		}
	}

	if isUnique {
		res = append(res, fmt.Sprintf("create unique index %s on %s (%s)", indexName, tableName, strings.Join(fields, ", ")))
	} else {
		res = append(res, fmt.Sprintf("create index %s on %s (%s)", indexName, tableName, strings.Join(fields, ", ")))
	}

	var e error
	for _, cmdTxt := range res {
		dbflex.Logger().Info(cmdTxt)
		if c.IsTx() {
			_, e = c.tx.Exec(cmdTxt)
		} else {
			_, e = c.db.Exec(cmdTxt)
		}
		if e != nil {
			return fmt.Errorf("error: %s command: %s", e.Error(), cmdTxt)
		}
	}

	return nil
}
