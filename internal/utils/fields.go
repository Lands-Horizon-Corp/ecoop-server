package utils

import (
	"fmt"
	"reflect"
	"time"
)

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
	if t, ok := f.Interface().(time.Time); ok {
		return t.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("%v", f.Interface())
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
