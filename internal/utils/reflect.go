package utils

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

var timeType = reflect.TypeFor[time.Time]()

// Peek is the value at idx inside v, or false when a pointer on the way there
// is nil. Unlike bun's own accessor it never allocates, so reading a model
// whose embedded struct is a nil pointer neither panics nor changes it.
func Peek(v reflect.Value, idx []int) (reflect.Value, bool) {
	for _, i := range idx {
		for v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v, true
}

// KeyString renders a key value as a stable string for grouping.
func KeyString(v reflect.Value) string {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)
	}
	if v.CanInterface() {
		if t, ok := v.Interface().(time.Time); ok {
			return t.UTC().Format(time.RFC3339Nano)
		}
		return fmt.Sprint(v.Interface())
	}
	return ""
}

// Orderable reports whether a type can be a version: values that only grow.
func Orderable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return true
	}
	return t == timeType
}

// VersionKey turns an integer or time version into an exactly comparable
// number. Times count microseconds (what Postgres stores), which cannot
// overflow for any date Postgres accepts, unlike nanoseconds.
func VersionKey(v reflect.Value) (int64, bool) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, true
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), true
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return int64(v.Uint()), true
	}
	if v.CanInterface() {
		if t, ok := v.Interface().(time.Time); ok {
			if t.IsZero() {
				return 0, true // no time at all is the oldest version
			}
			return t.UnixMicro(), true
		}
	}
	return 0, false
}

// FitsInteger reports whether n can be held by the integer kind k without
// wrapping around. bun's scanners convert with SetInt/SetUint, which silently
// truncate: 40000 in an int16 would become -25536.
func FitsInteger(k reflect.Kind, n int64) bool {
	switch k {
	case reflect.Int8:
		return n >= math.MinInt8 && n <= math.MaxInt8
	case reflect.Int16:
		return n >= math.MinInt16 && n <= math.MaxInt16
	case reflect.Int32:
		return n >= math.MinInt32 && n <= math.MaxInt32
	case reflect.Uint8:
		return n >= 0 && n <= math.MaxUint8
	case reflect.Uint16:
		return n >= 0 && n <= math.MaxUint16
	case reflect.Uint32:
		return n >= 0 && n <= math.MaxUint32
	case reflect.Uint, reflect.Uint64, reflect.Uintptr:
		return n >= 0
	}
	return true // int, int64: every int64 fits
}

// IsIntegerKind reports whether k is a signed or unsigned integer kind.
func IsIntegerKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return true
	}
	return false
}
