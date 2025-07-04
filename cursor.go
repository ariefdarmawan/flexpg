package flexpg

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"git.kanosolution.net/kano/dbflex/drivers/rdbms"
	"github.com/sebarcode/codekit"
)

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	rdbms.Cursor
}

func (c *Cursor) CastValue(value interface{}, refType reflect.Type) (interface{}, error) {
	var d interface{}
	var err error

	typeName := ""
	if refType != nil {
		typeName = refType.String()
	}

	func() {
		if r := recover(); r != nil {
			err = errors.New(r.(string))
		}

		valueTypeString := ""
		rv := reflect.ValueOf(value)
		rvk := rv.Kind()
		switch rvk {
		case reflect.Invalid:
			d = value

		default:
			if rv.IsZero() {
				valueTypeString = ""
			} else {
				valueTypeString = rv.Type().String()
			}
		}

		if typeName == "" && valueTypeString != "" {
			switch v := value.(type) {
			case int:
				d = v

			case int8:
				d = int(v)

			case int16:
				d = int(v)

			case int32:
				d = int(v)

			case int64:
				d = int(v)

			case float32:
				d = v

			case float64:
				d = v

			case time.Time:
				d = v

			case *time.Time:
				d = v

			default:
				d, err = processByTypeName(value, refType, typeName)
			}
		} else {
			d, err = processByTypeName(value, refType, typeName)
		}
	}()

	return d, err
}

func processByTypeName(value interface{}, refType reflect.Type, typeName string) (interface{}, error) {
	var (
		d   interface{}
		err error
	)

	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Invalid {
		return value, nil
	}
	str := ""
	refTypeString := rv.Type().String()
	if refTypeString == typeName {
		return value, nil
	} else if strings.HasPrefix(refTypeString, "[]uint") {
		str = string(value.([]byte))
	} else if refTypeString == typeName {
		return value, nil
	} else if refTypeString != "interface{}" && strings.HasPrefix(refTypeString, "int") {
		str = fmt.Sprintf("%d", value)
	} else if strings.HasPrefix(refTypeString, "float") {
		str = fmt.Sprintf("%f", value)
	} else if refTypeString == "time.Time" || refTypeString == "*time.Time" {
		str = fmt.Sprintf("%s", value)
	} else {
		switch v := value.(type) {
		case string:
			str = v

		case []uint8:
			str = string(value.([]byte))
		}
	}

	switch typeName {
	case "codekit.M":
		if str == "null" {
			d = nil
		} else {
			m := codekit.M{}
			err = codekit.UnjsonFromString(str, &m)
			d = m
		}

	case "int", "int8", "int16", "int32":
		d, err = strconv.Atoi(str)

	case "time.Time", "*time.Time":
		dateFormat := "yyyy-MM-dd HH:mm:ss TH"
		d = codekit.String2Date(str, dateFormat)

	case "float32":
		d, err = strconv.ParseFloat(str, 32)

	case "float64":
		d, err = strconv.ParseFloat(str, 64)

	case "string":
		d = str

	default:
		if refType == nil {
			d = str
			break
		}

		// create buffer
		var refSlice reflect.Type

		isPtr := refType.Kind() == reflect.Ptr
		refTypeElem := refType
		if isPtr {
			refTypeElem = refType.Elem()
		}
		isStruct := refTypeElem.Kind() == reflect.Struct
		isMap := refTypeElem.Kind() == reflect.Map
		isSlice := refTypeElem.Kind() == reflect.Slice
		if isSlice {
			refSlice = refTypeElem
		}

		if isStruct {
			refTarget := createPtrFromType(refTypeElem).Interface()
			err = json.Unmarshal([]byte(str), refTarget)
			if err != nil {
				return d, err
			}

			if isPtr {
				d = refTarget
			} else {
				d = reflect.ValueOf(refTarget).Elem().Interface()
			}
		} else if isSlice {
			refArray := createPtrFromType(refSlice).Interface()
			err = json.Unmarshal([]byte(str), refArray)
			if err != nil {
				return d, err
			}

			if isPtr {
				d = refArray
			} else {
				d = reflect.ValueOf(refArray).Elem().Interface()
			}
		} else if isMap {
			refTarget := createPtrFromType(refTypeElem).Interface()
			err = json.Unmarshal([]byte(str), refTarget)
			if err != nil {
				return d, err
			}

			if isPtr {
				d = refTarget
			} else {
				d = reflect.ValueOf(refTarget).Elem().Interface()
			}
		}
	}
	return d, err
}

func createPtrFromType(t reflect.Type) reflect.Value {
	isPtr := t.Kind() == reflect.Ptr
	elemType := t
	if isPtr {
		elemType = elemType.Elem()
	}

	return reflect.New(elemType)
}
