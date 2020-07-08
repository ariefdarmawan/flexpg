package flexpg

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"text/template"

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

	dbflex.Logger().Debugf("SQL Command: %v", cmdtxt)
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
		//toolkit.Printfn("\nCmd: %s", cmdtxt)

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

func (q *Query) BuildFilter(f *dbflex.Filter) (interface{}, error) {
	if f == nil {
		return "", nil
	}

	ret := ""

	sqlfmts := []string{}
	if f.Value != nil {
		_, _, _, sqlfmts = ParseSQLMetadata(&q.Query,
			f.Value)
	}

	switch f.Op {
	case dbflex.OpAnd, dbflex.OpOr:
		txts := []string{}
		for _, item := range f.Items {
			if txt, err := q.BuildFilter(item); err != nil {
				return ret, err
			} else {
				txts = append(txts, txt.(string))
			}
		}
		ret = strings.Join(txts, toolkit.IfEq(f.Op, dbflex.OpAnd, " and ", " or ").(string))

	case dbflex.OpContains:
		v := ""
		if len(sqlfmts) > 0 {
			v = strings.TrimSuffix(strings.TrimPrefix(sqlfmts[0], "'"), "'")
		}
		ret = f.Field + " ilike '%" + v + "%'"

	case dbflex.OpEndWith:
		ret = f.Field + " ilike '" + f.Value.(string) + "%'"

	case dbflex.OpEq:
		if strings.HasPrefix(sqlfmts[0], "'") {
			ret = f.Field + " = " + sqlfmts[0]
		} else {
			ret = f.Field + " = '" + sqlfmts[0] + "'"
		}

	case dbflex.OpGt:
		ret = f.Field + " > " + sqlfmts[0]

	case dbflex.OpGte:
		ret = f.Field + " >= " + sqlfmts[0]

	case dbflex.OpLt:
		ret = f.Field + " < " + sqlfmts[0]

	case dbflex.OpLte:
		ret = f.Field + " <= " + sqlfmts[0]

	case dbflex.OpIn:
		items := []string{}
		for _, v := range sqlfmts {
			item := ""
			if strings.HasPrefix(sqlfmts[0], "'") {
				item = f.Field + " like " + v
			} else {
				item = f.Field + " = " + v
			}
			items = append(items, item)
		}
		ret = strings.Join(items, " or ")

	case dbflex.OpNe:
		if strings.HasPrefix(sqlfmts[0], "'") {
			ret = f.Field + " not like " + sqlfmts[0]
		} else {
			ret = f.Field + " != " + sqlfmts[0]
		}

	case dbflex.OpNin:
		items := []string{}
		for _, v := range sqlfmts {
			item := ""
			if strings.HasPrefix(sqlfmts[0], "'") {
				item = f.Field + " not like " + v
			} else {
				item = f.Field + " != " + v
			}
			items = append(items, item)
		}
		ret = strings.Join(items, " and ")

	case dbflex.OpRange:
		ret = f.Field + " between " + sqlfmts[0] + " and " + sqlfmts[1]
	}

	return ret, nil
}

//ParseSQLMetadata returns names, types, values and sql value as string
func ParseSQLMetadata(
	qr rdbms.RdbmsQuery,
	o interface{}) ([]string, []reflect.Type, []interface{}, []string) {
	names := []string{}
	types := []reflect.Type{}
	values := []interface{}{}
	sqlvalues := []string{}

	if toolkit.IsNil(o) {
		return names, types, values, sqlvalues
	}

	r := reflect.Indirect(reflect.ValueOf(o))
	t := r.Type()

	if r.Kind() == reflect.Struct && t.String() == "time.Time" {
		sqlvalues = append(sqlvalues, qr.ValueToSQlValue(o))
	} else if r.Kind() == reflect.Struct {
		nf := r.NumField()
		for fieldIdx := 0; fieldIdx < nf; fieldIdx++ {
			f := r.Field(fieldIdx)
			ft := t.Field(fieldIdx)
			if !f.CanSet() {
				continue
			}
			v := f.Interface()
			sqlname, ok := ft.Tag.Lookup(toolkit.TagName())
			if sqlname == "-" {
				continue
			}
			if ok && sqlname != "" {
				names = append(names, sqlname)
			} else {
				names = append(names, ft.Name)
			}
			types = append(types, ft.Type)
			values = append(values, v)
			if qr != nil {
				sqlvalues = append(sqlvalues, qr.ValueToSQlValue(v))
			}
		}
	} else if r.Kind() == reflect.Map {
		keys := r.MapKeys()
		for _, k := range keys {
			names = append(names, toolkit.Sprintf("%v", k.Interface()))
			types = append(types, k.Type())

			value := r.MapIndex(k)
			v := value.Interface()
			values = append(values, v)
			if qr != nil {
				sqlvalues = append(sqlvalues, qr.ValueToSQlValue(v))
			}
		}
	} else if r.Kind() == reflect.Slice {
		for i := 0; i < r.Len(); i++ {
			names = append(names, r.Type().Name())
			types = append(types, r.Type())

			value := r.Index(i)
			v := value.Interface()
			values = append(values, v)
			if qr != nil {
				sqlvalues = append(sqlvalues, qr.ValueToSQlValue(v))
			}
		}
	} else if r.Kind() == reflect.Slice {
		for i := 0; i < r.Len(); i++ {
			if qr != nil {
				sqlvalues = append(sqlvalues, qr.ValueToSQlValue(r.Index(i)))
			}
		}
	} else {
		names = append(names, t.Name())
		types = append(types, t)
		values = append(values, o)
		if qr != nil {
			sqlvalues = append(sqlvalues, qr.ValueToSQlValue(o))
		}
	}

	return names, types, values, sqlvalues
}
func CleanupSQL(s string) string {
	return strings.Replace(s, "'", "''", -1)
}

func (qr *Query) ValueToSQlValue(v interface{}) string {
	if sqlvalue, found := Mapper.ToSQLValue(v); found {
		return sqlvalue
	} else {
		return toolkit.Sprintf("'%s'", CleanupSQL(fmt.Sprintf("%v", v)))
	}
	// switch v.(type) {
	// case int, int8, int16, int32, int64:
	// 	return toolkit.Sprintf("%d", v)
	// case float32, float64:
	// 	return toolkit.Sprintf("%f", v)
	// case time.Time:
	// 	return toolkit.Date2String(v.(time.Time), "'yyyy-MM-dd hh:mm:ss'")
	// case *time.Time:
	// 	dt := v.(*time.Time)
	// 	return toolkit.Date2String(*dt, "'yyyy-MM-dd hh:mm:ss'")
	// case bool:
	// 	if v.(bool) == true {
	// 		return "true"
	// 	}
	// 	return "false"
	// default:
	// 	return toolkit.Sprintf("'%s'", CleanupSQL(fmt.Sprintf("%v", v)))
	// }
}

func executeTemplate(templateTxt string, data toolkit.M) string {
	var buff bytes.Buffer
	tmp, err := template.New("main").Parse(templateTxt)
	if err != nil {
		return templateTxt
	}
	err = tmp.Execute(&buff, data)
	if err != nil {
		return templateTxt
	}
	return buff.String()
}

// BuildPartial so frustation how to extend this lib Ffffffffffffffffuuuuuuuuuuuuuu
func (q *Query) BuildPartial(querytype string, value interface{}) string {
	templates := q.Templates()
	switch querytype {
	case dbflex.QuerySQL:
		if v, ok := value.(string); ok {
			return v
		}
	case dbflex.QuerySelect:
		if v, ok := value.([]string); ok {
			v2 := []string{}
			for _, cleanv := range v {
				cleanv = strings.TrimSpace(cleanv)
				if cleanv != "" {
					v2 = append(v2, cleanv)
				}
			}
			if len(v2) > 0 {
				return strings.Join(v2, ", ")
			}
		}
		return "*"
	case dbflex.QueryTake:
		if v, ok := value.(int); ok {
			return executeTemplate(templates[dbflex.QueryTake], toolkit.M{}.Set(dbflex.QueryTake, v))
		}
	case dbflex.QuerySkip:
		if v, ok := value.(int); ok {
			return executeTemplate(templates[dbflex.QuerySkip], toolkit.M{}.Set(dbflex.QuerySkip, v))
		}
	case dbflex.QueryOrder:
		if v, ok := value.([]string); ok {
			fields := []string{}
			orderfields := v
			for _, orderfield := range orderfields {
				if !strings.HasPrefix(orderfield, "-") {
					fields = append(fields, strings.TrimSpace(orderfield))
				} else {
					orderfield = orderfield[1:]
					fields = append(fields, strings.TrimSpace(orderfield)+" desc")
				}
			}
			if len(fields) > 0 {
				return executeTemplate(templates[dbflex.QueryOrder], toolkit.M{}.Set(dbflex.QueryOrder, strings.Join(fields, " ")))
			}
		}
	case dbflex.QueryGroup:
		if v, ok := value.([]string); ok {
			groupbyStr := func() string {
				s := "GROUP BY "
				fields := []string{}
				gs := v
				for _, g := range gs {
					if strings.TrimSpace(g) != "" {
						fields = append(fields, g)
					}
				}
				if len(fields) == 0 {
					return ""
				}
				return s + strings.Join(fields, ",")
			}()
			return executeTemplate(templates[dbflex.QueryGroup], toolkit.M{}.Set(dbflex.QueryOrder, groupbyStr))
		}
	}
	return ""
}
