package utils

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

func BunColumnFieldIndex[T any](column string) int {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Struct {
		return -1
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("bun")
		if tag == "" || tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == column {
			return i
		}
	}
	return -1
}

func FieldValueAt[TData any](data *TData, idx int) string {
	if data == nil || idx < 0 {
		return ""
	}
	v := reflect.ValueOf(data).Elem()
	if idx >= v.NumField() {
		return ""
	}
	f := v.Field(idx)
	if !f.CanInterface() {
		return ""
	}
	return formatFieldValue(f)
}

func FieldIsNilAt[TData any](data *TData, idx int) bool {
	if data == nil || idx < 0 {
		return true
	}
	v := reflect.ValueOf(data).Elem()
	if idx >= v.NumField() {
		return true
	}
	f := v.Field(idx)
	switch f.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return !f.CanInterface() || f.IsNil()
	default:
		return false
	}
}

func formatFieldValue(f reflect.Value) string {
	switch f.Kind() {
	case reflect.String:
		return f.String()
	case reflect.Pointer, reflect.Interface:
		if f.IsNil() {
			return ""
		}
		return formatFieldValue(f.Elem())
	case reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		if f.IsNil() {
			return ""
		}
	}
	// %v prints a time with its zone abbreviation ("+0800 PST" for Manila), which Postgres reads as
	// a different zone or rejects; RFC 3339 in UTC is unambiguous.
	if t, ok := f.Interface().(time.Time); ok {
		return t.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("%v", f.Interface())
}
