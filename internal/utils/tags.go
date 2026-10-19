package utils

import (
	"reflect"
	"strings"
)

func StructTagFieldIndex[T any](tagName, columnName string) int {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Struct {
		return -1
	}
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get(tagName)
		if tag == "" || tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == columnName {
			return i
		}
	}
	return -1
}
