package flexpg

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	"github.com/eaciit/toolkit"
)

// Query implementaion of dbflex.IQuery
type Query struct {
	rdbms.Query
	conn       *Connection
	sqlcommand string
}

// Cursor produces a cursor from query
func (q *Query) Cursor(in toolkit.M) dbflex.ICursor {
	cursor := new(Cursor)
	cursor.SetThis(cursor)

	ct := q.Config(dbflex.ConfigKeyCommandType, dbflex.QuerySelect).(string)
	if ct != dbflex.QuerySelect && ct != dbflex.QuerySQL {
		cursor.SetError(toolkit.Errorf("cursor is used for only select command"))
		return cursor
	}

	cmdtxt := q.Config(dbflex.ConfigKeyCommand, "").(string)
	if cmdtxt == "" {
		cursor.SetError(toolkit.Errorf("no command"))
		return cursor
	}

	tablename := q.Config(dbflex.ConfigKeyTableName, "").(string)
	cq := dbflex.From(tablename).Select("count(*) as Count")
	if filter := q.Config(dbflex.ConfigKeyFilter, nil); filter != nil {
		cq.Where(filter.(*dbflex.Filter))
	}
	cursor.SetCountCommand(cq)

	var (
		rows *sql.Rows
		err  error
	)
	if q.conn.IsTx() {
		rows, err = q.conn.tx.Query(cmdtxt)
	} else {
		rows, err = q.conn.db.Query(cmdtxt)
	}
	if rows == nil {
		cursor.SetError(toolkit.Errorf("%s. SQL Command: %s", err.Error(), cmdtxt))
	} else {
		cursor.SetFetcher(rows)
	}
	return cursor
}

// Execute will executes non-select command of a query
func (q *Query) Execute(in toolkit.M) (interface{}, error) {
	cmdtype, ok := q.Config(dbflex.ConfigKeyCommandType, dbflex.QuerySelect).(string)
	if !ok {
		return nil, toolkit.Errorf("Operation is unknown. current operation is %s", cmdtype)
	}
	cmdtxt := q.Config(dbflex.ConfigKeyCommand, "").(string)
	if cmdtxt == "" {
		return nil, toolkit.Errorf("No command")
	}

	var (
		sqlfieldnames []string
		sqlvalues     []string
	)

	data, hasData := in["data"]
	if !hasData && !(cmdtype == dbflex.QueryDelete || cmdtype == dbflex.QuerySelect) {
		return nil, toolkit.Error("non select and delete command should has data")
	}

	if hasData {
		sqlfieldnames, _, _, sqlvalues = rdbms.ParseSQLMetadata(q, data)
		affectedfields := q.Config("fields", []string{}).([]string)
		if len(affectedfields) > 0 {
			newfieldnames := []string{}
			newvalues := []string{}
			for idx, field := range sqlfieldnames {
				for _, find := range affectedfields {
					if strings.ToLower(field) == strings.ToLower(find) {
						newfieldnames = append(newfieldnames, find)
						newvalues = append(newvalues, sqlvalues[idx])
					}
				}
			}
			sqlfieldnames = newfieldnames
			sqlvalues = newvalues
		}
	}

	switch cmdtype {
	case dbflex.QueryInsert:
		cmdtxt = strings.Replace(cmdtxt, "{{.FIELDS}}", strings.Join(sqlfieldnames, ","), -1)
		cmdtxt = strings.Replace(cmdtxt, "{{.VALUES}}", strings.Join(sqlvalues, ","), -1)
		toolkit.Printfn("\nCmd: %s", cmdtxt)

	case dbflex.QueryUpdate:
		//fmt.Println("fieldnames:", sqlfieldnames)
		updatedfields := []string{}
		for idx, fieldname := range sqlfieldnames {
			updatedfields = append(updatedfields, fieldname+"="+sqlvalues[idx])
		}
		cmdtxt = strings.Replace(cmdtxt, "{{.FIELDVALUES}}", strings.Join(updatedfields, ","), -1)
	}

	//fmt.Println("Cmd: ", cmdtxt)
	var (
		r   sql.Result
		err error
	)
	if q.conn.IsTx() {
		r, err = q.conn.tx.Exec(cmdtxt)
	} else {
		r, err = q.conn.db.Exec(cmdtxt)
	}

	if err != nil {
		return nil, toolkit.Errorf("%s. SQL Command: %s", err.Error(), cmdtxt)
	}
	return r, nil
}

// ExecType to identify type of exec
type ExecType int

const (
	ExecQuery ExecType = iota
	ExecNonQuery
	ExecQueryRow
)

/*
func (q *Query) SQL(string cmd, exec) dbflex.IQuery {
	swicth()
}
*/

func CleanupSQL(s string) string {
	return strings.Replace(s, "'", "''", -1)
}

func tsValue(dt time.Time) string {
	if dt.IsZero() {
		return toolkit.Date2String(dt.Local(), "'yyyy-MM-dd HH:mm:ss T'")
	}
	return toolkit.Date2String(dt, "'yyyy-MM-dd HH:mm:ss T'")
}

func (qr *Query) ValueToSQlValue(v interface{}) string {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return toolkit.Sprintf("%d", v)
	case float32, float64:
		return toolkit.Sprintf("%f", v)
	case time.Time:
		return tsValue(v.(time.Time))
	case *time.Time:
		dt := v.(*time.Time)
		return tsValue(*dt)
	case bool:
		if v.(bool) == true {
			return "true"
		}
		return "false"
	case string:
		return toolkit.Sprintf("'%s'", CleanupSQL(v.(string)))
	default:
		return toolkit.Sprintf("'%s'", CleanupSQL(fmt.Sprintf("%v", toolkit.JsonString(v))))
	}
}
