package cqrs

import (
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"
)

// Postgres text cannot hold NUL bytes or invalid UTF-8, and bun inlines values into the SQL text:
// a NUL truncates the statement and invalid UTF-8 is silently replaced with U+FFFD. Both are
// rejected before the write so stored data is always exactly what the caller sent.

// textFieldIndexes lists the fields of TData that can carry text: strings, *string, []string and
// maps (JSON documents). It is computed once per service.
func textFieldIndexes[TData any]() []int {
	t := reflect.TypeFor[TData]()
	if t.Kind() != reflect.Struct {
		return nil
	}
	var out []int
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		ft := f.Type
		if ft.Kind() == reflect.Pointer || ft.Kind() == reflect.Slice {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.String || f.Type.Kind() == reflect.Map {
			out = append(out, i)
		}
	}
	return out
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) checkText(data *TData) error {
	if len(c.textFields) == 0 || data == nil {
		return nil
	}
	v := reflect.ValueOf(data).Elem()
	for _, i := range c.textFields {
		if err := checkTextValue(v.Field(i)); err != nil {
			return fmt.Errorf("%w: field %s: %w", ErrInvalidText, v.Type().Field(i).Name, err)
		}
	}
	return nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) checkTexts(data []TData) error {
	for i := range data {
		if err := c.checkText(&data[i]); err != nil {
			return fmt.Errorf("record %d: %w", i, err)
		}
	}
	return nil
}

func checkTextValue(v reflect.Value) error {
	switch v.Kind() {
	case reflect.String:
		return checkString(v.String())
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return checkTextValue(v.Elem())
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil // bytes are stored as bytea
		}
		for i := range v.Len() {
			if err := checkTextValue(v.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			if err := checkTextValue(iter.Key()); err != nil {
				return err
			}
			if err := checkTextValue(iter.Value()); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkString(s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("invalid UTF-8")
	}
	if strings.IndexByte(s, 0) >= 0 {
		return fmt.Errorf("contains a NUL byte")
	}
	return nil
}
