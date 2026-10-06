package cqrs

import (
	"fmt"
	"reflect"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
)

// Text written through the service is checked and cleaned before any SQL runs (sql.CleanText):
// invalid UTF-8 and NUL bytes are always refused; unless RawText is set, strings are also normalized
// to NFC and U+FFFD, control and BiDi override characters are refused.

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

// prepareText validates and normalizes the text fields of data in place (data is the service's own
// copy of the record).
func (c *CQRSService[TData, TResponse, TRequest, TID]) prepareText(data *TData) error {
	if len(c.textFields) == 0 || data == nil {
		return nil
	}
	v := reflect.ValueOf(data).Elem()
	for _, i := range c.textFields {
		clean, err := sqlsvc.CleanText(v.Field(i), sqlsvc.TextPolicy{Normalize: !c.RawText, RejectUnsafe: !c.RawText})
		if err != nil {
			return fmt.Errorf("field %s: %w", v.Type().Field(i).Name, err)
		}
		v.Field(i).Set(clean)
	}
	return nil
}

// prepareTexts is prepareText for a batch; it returns a copy so the caller's slice is untouched.
func (c *CQRSService[TData, TResponse, TRequest, TID]) prepareTexts(data []TData) ([]TData, error) {
	if len(c.textFields) == 0 {
		return data, nil
	}
	out := make([]TData, len(data))
	copy(out, data)
	for i := range out {
		if err := c.prepareText(&out[i]); err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
	}
	return out, nil
}
