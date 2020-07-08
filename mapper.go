package flexpg

import (
	"database/sql"
	"reflect"
	"strings"
	"time"

	"github.com/eaciit/toolkit"
)

func init() {
	RegisterMapperFilter("default", new(DefaultMapper))
	RegisterMapperMarshal("default", new(DefaultMapper))
	RegisterMapperMigration("default", new(DefaultMapper))
}

type IMapperMigration interface {
	MigrationDBType(f reflect.StructField) (dbtype string)
	// MigrationDBTypeUpdateCompare()
	MigrationDBTypeDefaultNotNull(f reflect.StructField, dbtype string) (defaultvalue string, found bool)
}

type IMapperSerialization interface {
	// Because of dbflex behavior, serialize will divide 2 step by fieldtype
	PrepareScanValue(ct *sql.ColumnType) (fieldtype string)
	ScanValue(m *toolkit.M, fieldtype, columnname string, value interface{}) bool
	// value to sql value
	ToSQLValue(v interface{}) (string, bool)
}

type IMapperFilter interface {
	Filter()
}

var (
	MapperMigration = map[string]IMapperMigration{}
	MapperMarshal   = map[string]IMapperSerialization{}
	MapperFilter    = map[string]IMapperFilter{}
	Mapper          = MapperManager{}
)

func RegisterMapperMigration(k string, m IMapperMigration) {
	if m == nil {
		return
	}
	MapperMigration[k] = m
}

func RegisterMapperMarshal(k string, m IMapperSerialization) {
	if m == nil {
		return
	}
	MapperMarshal[k] = m
}

func RegisterMapperFilter(k string, m IMapperFilter) {
	if m == nil {
		return
	}
	MapperFilter[k] = m
}

type MapperManager struct{}
type DefaultMapper struct{}

func (b *MapperManager) MigrationDBType(f reflect.StructField) string {
	for _, fn := range MapperMigration {
		if dbtype := fn.MigrationDBType(f); dbtype != "" {
			return dbtype
		}
	}
	return ""
}

func (b *MapperManager) MigrationDBTypeDefaultNotNull(f reflect.StructField, dbtype string) (string, bool) {
	for _, fn := range MapperMigration {
		if defaultvalue, found := fn.MigrationDBTypeDefaultNotNull(f, dbtype); found {
			return defaultvalue, found
		}
	}
	return "", false
}

func (b *MapperManager) PrepareScanValue(ct *sql.ColumnType) string {
	for _, fn := range MapperMarshal {
		if fieldtype := fn.PrepareScanValue(ct); fieldtype != "" {
			return fieldtype
		}
	}
	return ""
}

func (b *MapperManager) ScanValue(m *toolkit.M, fieldtype, columnname string, value interface{}) bool {
	for _, fn := range MapperMarshal {
		if done := fn.ScanValue(m, fieldtype, columnname, value); done {
			return done
		}
	}
	return false
}

func (b *MapperManager) ToSQLValue(v interface{}) (string, bool) {
	for _, fn := range MapperMarshal {
		if sqlvalue, done := fn.ToSQLValue(v); done {
			return sqlvalue, done
		}
	}
	return "", false
}

func (b *MapperManager) Filter() {
	for _, fn := range MapperFilter {
		fn.Filter()
	}
}

func (d *DefaultMapper) MigrationDBType(f reflect.StructField) (dbType string) {
	def := f.Tag.Get("dbflex-migration-sqltype") // high-priority
	if def != "" {
		return def
	}
	fieldType := f.Type.String()
	if fieldType == "string" {
		dbType = "text"
	} else if fieldType != "interface{}" && (fieldType == "int" || fieldType == "int64") {
		dbType = "bigint" // by default use bigint instead int, because in windows golang int = int64 but postgre int = int32
	} else if fieldType != "interface{}" && strings.HasPrefix(fieldType, "int") {
		dbType = "integer"
	} else if strings.Contains(fieldType, "time.Time") {
		dbType = "timestamptz"
	} else if fieldType == "float32" {
		dbType = "numeric (32,8)"
	} else if fieldType == "float64" {
		dbType = "numeric (64,8)"
	} else if fieldType == "bool" {
		dbType = "boolean"
	}
	return
}

func (d *DefaultMapper) MigrationDBTypeUpdateCompare() {
}

func (d *DefaultMapper) MigrationDBTypeDefaultNotNull(f reflect.StructField, dbtype string) (string, bool) {
	defaultvalue := f.Tag.Get("dbflex-migration-default") // high-priority
	if defaultvalue != "" {
		return defaultvalue, true
	}
	switch dbtype {
	case "text":
		return "''", true
	case "bigint":
		return "0", true
	case "integer":
		return "0", true
	case "numeric (64,8)":
		return "0", true
	case "numeric (32,8)":
		return "0", true
	case "boolean":
		return "'F'", true
	case "timestamptz":
		return "'1900-01-01 00:00:00'", true
	}
	return "", false
}

func (d *DefaultMapper) PrepareScanValue(ct *sql.ColumnType) (fieldtype string) {
	typename := strings.ToLower(ct.DatabaseTypeName())
	if strings.HasPrefix(typename, "int") {
		return "int"
	} else if strings.HasPrefix(typename, "dec") || strings.HasPrefix(typename, "float") ||
		strings.HasPrefix(typename, "number") || strings.HasPrefix(typename, "num") {
		return "float64"
	} else if strings.HasPrefix(typename, "date") || strings.HasPrefix(typename, "time") {
		return "time.Time"
	}
	return ""
}

func (d *DefaultMapper) ScanValue(m *toolkit.M, fieldtype, columnname string, value interface{}) bool {
	v, ok := value.([]byte)
	if ok {
		switch fieldtype {
		case "int":
			m.Set(columnname, toolkit.ToInt(string(v), toolkit.RoundingAuto))
			return true

		case "float64":
			m.Set(columnname, toolkit.ToFloat64(string(v), 4, toolkit.RoundingAuto))
			return true

		case "time.Time":
			if dt, err := time.Parse(time.RFC3339, string(v)); err == nil {
				m.Set(columnname, dt)
			} else {
				dt = toolkit.String2Date(string(v), TimeFormat())
				m.Set(columnname, dt)
			}
			return true
		}
	}
	return false
}

func (d *DefaultMapper) ToSQLValue(v interface{}) (string, bool) {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return toolkit.Sprintf("%d", v), true
	case float32, float64:
		return toolkit.Sprintf("%f", v), true
	case time.Time:
		return toolkit.Date2String(v.(time.Time), "'yyyy-MM-dd hh:mm:ss'"), true
	case *time.Time:
		dt := v.(*time.Time)
		return toolkit.Date2String(*dt, "'yyyy-MM-dd hh:mm:ss'"), true
	case bool:
		if v.(bool) == true {
			return "true", true
		}
		return "false", true
	}
	return "", false
}

func (d *DefaultMapper) Filter() {
}
