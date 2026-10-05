package pagination

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
)

type cursorPayload struct {
	Values   []string `json:"v"`
	Null     []bool   `json:"n,omitempty"`
	Backward bool     `json:"b,omitempty"`
}

func (c *PaginationService[TData, TID]) resolveSortFields(
	sortFields []database.SortField,
) ([]database.SortField, error) {
	resolved := make([]database.SortField, 0, len(sortFields)+1)
	if len(sortFields) == 0 {
		resolved = append(resolved, c.defaultSortField())
	} else {
		for _, sf := range sortFields {
			if utils.BunColumnFieldIndex[TData](sf.Field) == -1 {
				return nil, fmt.Errorf("unknown sort field %q", sf.Field)
			}
			if sf.Order != database.SortOrderAsc && sf.Order != database.SortOrderDesc {
				sf.Order = database.SortOrderAsc
			}
			resolved = append(resolved, sf)
		}
	}
	for _, sf := range resolved {
		if sf.Field == c.ColumnDefaultID {
			return resolved, nil
		}
	}
	return append(resolved, database.SortField{Field: c.ColumnDefaultID, Order: database.SortOrderDesc}), nil
}

func (c *PaginationService[TData, TID]) defaultSortField() database.SortField {
	sf := database.SortField{Field: c.ColumnDefaultID, Order: database.SortOrderDesc}
	parts := strings.Fields(c.ColumnDefaultSort)
	if len(parts) == 0 {
		return sf
	}
	sf.Field = parts[0]
	if len(parts) > 1 && strings.EqualFold(parts[1], "asc") {
		sf.Order = database.SortOrderAsc
	}
	return sf
}

func (c *PaginationService[TData, TID]) encodeCursor(
	data *TData, sortFields []database.SortField, backward bool,
) (string, error) {
	values := make([]string, len(sortFields))
	nulls := make([]bool, len(sortFields))
	for i, sf := range sortFields {
		idx := utils.BunColumnFieldIndex[TData](sf.Field)
		values[i] = utils.FieldValueAt(data, idx)
		nulls[i] = utils.FieldIsNilAt(data, idx)
	}
	return utils.EncodeQueryParam(cursorPayload{Values: values, Null: nulls, Backward: backward})
}

func (c *PaginationService[TData, TID]) decodeCursor(
	cursor *string, sortFields []database.SortField,
) (payload cursorPayload, ok bool, err error) {
	if cursor == nil || *cursor == "" {
		return cursorPayload{}, false, nil
	}
	payload, err = utils.DecodeQueryParam[cursorPayload](*cursor)
	if err != nil {
		return cursorPayload{}, false, fmt.Errorf("decoding cursor: %w", err)
	}
	if len(payload.Values) != len(sortFields) {
		return cursorPayload{}, false, fmt.Errorf(
			"cursor does not match the current sort fields: expected %d values, got %d",
			len(sortFields), len(payload.Values),
		)
	}
	if len(payload.Null) < len(payload.Values) {
		payload.Null = append(payload.Null, make([]bool, len(payload.Values)-len(payload.Null))...)
	}
	return payload, true, nil
}

func cursorOperator(order database.SortOrder, backward bool) string {
	lessThan := order == database.SortOrderDesc
	if backward {
		lessThan = !lessThan
	}
	if lessThan {
		return "<"
	}
	return ">"
}

func cursorIsUniform(sortFields []database.SortField, backward bool) (op string, uniform bool) {
	if len(sortFields) == 0 {
		return "", true
	}
	op = cursorOperator(sortFields[0].Order, backward)
	for _, sf := range sortFields[1:] {
		if cursorOperator(sf.Order, backward) != op {
			return "", false
		}
	}
	return op, true
}

func applyCursorUniform(
	q *bun.SelectQuery, sortFields []database.SortField, values []string, op string,
) *bun.SelectQuery {
	n := len(sortFields)
	args := make([]any, 0, n*2)
	for _, sf := range sortFields {
		args = append(args, bun.Ident(sf.Field))
	}
	for _, v := range values {
		args = append(args, v)
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
	return q.Where(fmt.Sprintf("(%s) %s (%s)", placeholders, op, placeholders), args...)
}

func appendCursorTerm(
	q *bun.SelectQuery, sortFields []database.SortField, values []string, nulls []bool, idx int, backward bool,
) *bun.SelectQuery {
	for i := range idx {
		field := bun.Ident(sortFields[i].Field)
		if nulls[i] {
			q = q.Where("? IS NULL", field)
		} else {
			q = q.Where("? = ?", field, values[i])
		}
	}
	field := bun.Ident(sortFields[idx].Field)
	if nulls[idx] {
		if backward {
			return q.Where("? IS NOT NULL", field)
		}
		return q.Where("1 = 0")
	}
	op := cursorOperator(sortFields[idx].Order, backward)
	if !backward {
		return q.Where(fmt.Sprintf("(? %s ? OR ? IS NULL)", op), field, values[idx], field)
	}
	return q.Where(fmt.Sprintf("? %s ?", op), field, values[idx])
}

func anyNullableSortField[TData any](sortFields []database.SortField) bool {
	t := reflect.TypeFor[TData]()
	for _, sf := range sortFields {
		idx := utils.BunColumnFieldIndex[TData](sf.Field)
		if idx < 0 || idx >= t.NumField() {
			continue
		}
		if t.Field(idx).Type.Kind() == reflect.Pointer {
			return true
		}
	}
	return false
}

func reverseSortFields(sortFields []database.SortField) []database.SortField {
	reversed := make([]database.SortField, len(sortFields))
	for i, sf := range sortFields {
		order := database.SortOrderDesc
		if sf.Order == database.SortOrderDesc {
			order = database.SortOrderAsc
		}
		reversed[i] = database.SortField{Field: sf.Field, Order: order}
	}
	return reversed
}
