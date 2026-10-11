package utils

import "reflect"

func IsNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface, reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return v.IsNil()
	}
	return false
}
