package utils

import (
	"reflect"
	"strings"

	"github.com/uptrace/bun/schema"
)

// IsJSONColumn reports whether the field holds a JSON document: a jsonb or json
// column, or a Go type that is a document (a map, a struct, a slice that is not
// bytes) rather than a scalar.
func IsJSONColumn(f *schema.Field) bool {
	if strings.Contains(strings.ToLower(f.CreateTableSQLType), "json") {
		return true
	}
	t := f.IndirectType
	switch t.Kind() {
	case reflect.Map, reflect.Struct, reflect.Interface:
		return t != timeType
	case reflect.Slice:
		return t.Elem().Kind() != reflect.Uint8
	}
	return false
}

// AmbiguousTemporal reports whether sqlType is a timestamp without time zone,
// a time of day or an interval: Debezium sends these as integers in a unit
// (milliseconds or microseconds) chosen by the column's precision.
func AmbiguousTemporal(sqlType string) bool {
	t := strings.ToLower(strings.TrimSpace(sqlType))
	if strings.Contains(t, "timestamptz") || strings.Contains(t, "timetz") ||
		(strings.Contains(t, "with time zone") && !strings.Contains(t, "without")) {
		return false
	}
	base, _, _ := strings.Cut(t, "(")
	switch strings.TrimSpace(base) {
	case "timestamp", "timestamp without time zone", "time", "time without time zone", "interval":
		return true
	}
	return false
}
