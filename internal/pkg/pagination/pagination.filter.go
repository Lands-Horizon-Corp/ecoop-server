package pagination

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) applyFilters(
	q *bun.SelectQuery, filterRoot StructuredFilter,
) (*bun.SelectQuery, error) {
	if len(filterRoot.Filters) == 0 {
		return q, nil
	}
	sep := "AND"
	if filterRoot.Logic == LogicOr {
		sep = "OR"
	}
	var groupErr error
	q = q.WhereGroup("AND", func(q *bun.SelectQuery) *bun.SelectQuery {
		for _, f := range filterRoot.Filters {
			if groupErr != nil {
				break
			}
			if f.Timezone == "" {
				f.Timezone = filterRoot.Timezone
			}
			wholeIndexSearch := f.Mode == ModeSearch && f.Field == ""
			isCustom := f.Mode == ModeCustom
			if !wholeIndexSearch && !isCustom && utils.StructTagFieldIndex[TData]("bun", f.Field) == -1 {
				groupErr = fmt.Errorf("%w: filter %q", ErrUnknownField, f.Field)
				break
			}
			q = q.WhereGroup(sep, func(inner *bun.SelectQuery) *bun.SelectQuery {
				newQ, err := c.applyTerm(inner, f)
				if err != nil {
					groupErr = fmt.Errorf("filter %q: %w", f.Field, err)
					return inner
				}
				return newQ
			})
		}
		return q
	})
	if groupErr != nil {
		return nil, groupErr
	}
	return q, nil
}

func (c *PaginationService[TData, TID]) applyTerm(q *bun.SelectQuery, f Filter) (*bun.SelectQuery, error) {
	if f.Mode != ModeCustom {
		return applyFilterTerm(q, f, c.ColumnDefaultID, c.columnKind(f.Field))
	}
	if f.Custom == nil {
		return nil, fmt.Errorf("%w: ModeCustom requires Filter.Custom", ErrInvalidFilter)
	}
	built, err := f.Custom(q, f.Value)
	if err != nil {
		return nil, err
	}
	if built == nil {
		return nil, fmt.Errorf("%w: custom filter returned a nil query", ErrInvalidFilter)
	}
	return built, nil
}

// columnKind reads how the entity stores the column (date, timestamptz, time...)
// from its bun schema; an unknown column is compared as an instant.
func (c *PaginationService[TData, TID]) columnKind(field string) (kind colKind) {
	defer func() {
		if recover() != nil {
			kind = kindInstant
		}
	}()
	if c.ReadSQLService == nil || c.ReadSQLService.Client() == nil {
		return kindInstant
	}
	table := c.ReadSQLService.Client().Table(reflect.TypeFor[TData]())
	if fld, ok := table.FieldMap[field]; ok {
		return kindOf(fld.CreateTableSQLType)
	}
	return kindInstant
}

func applyFilterTerm(q *bun.SelectQuery, f Filter, columnDefaultID string, kind colKind) (*bun.SelectQuery, error) {
	loc, err := resolveZone(f.Timezone)
	if err != nil {
		return nil, err
	}
	if f.Value != nil {
		clean, err := utils.CleanText(reflect.ValueOf(f.Value), utils.TextPolicy{Normalize: true})
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidFilter, err)
		}
		f.Value = clean.Interface()
	}
	col := bun.Ident(f.Field)
	switch f.Mode {
	case ModeEqual, ModeNotEqual, ModeGT, ModeGTE,
		ModeLT, ModeLTE, ModeBefore, ModeAfter,
		ModeContains, ModeNotContains, ModeStartsWith, ModeEndsWith,
		ModeSearch, ModeRange:
		if f.Value == nil {
			return nil, fmt.Errorf(
				"%w: mode %q requires a non-nil value (use ModeIsEmpty/ModeIsNotEmpty to match null/empty values instead)",
				ErrInvalidFilter, f.Mode,
			)
		}
	case ModeInside, ModeOutside:
		if f.Value == nil {
			return nil, fmt.Errorf("%w: mode %q requires a non-nil list value", ErrInvalidFilter, f.Mode)
		}
		if k := reflect.ValueOf(f.Value).Kind(); k != reflect.Slice && k != reflect.Array {
			return nil, fmt.Errorf("%w: mode %q requires a list value, got %T", ErrInvalidFilter, f.Mode, f.Value)
		}
		list := reflect.ValueOf(f.Value)
		for i := range list.Len() {
			if e := list.Index(i); e.Kind() == reflect.Interface && e.IsNil() {
				return nil, fmt.Errorf("%w: mode %q does not accept null list items (item %d)", ErrInvalidFilter, f.Mode, i)
			}
		}
	}
	if f.DataType == DataTypeTime {
		expr, args, ok, err := timeCondition(col, f, loc, kind)
		if err != nil {
			return nil, err
		}
		if ok {
			return q.Where(expr, args...), nil
		}
	}
	if f.DataType == DataTypeDate {
		expr, args, ok, err := dateCondition(col, f, loc, kind)
		if err != nil {
			return nil, err
		}
		if ok {
			return q.Where(expr, args...), nil
		}
	}
	switch f.Mode {
	case ModeEqual, ModeNotEqual, ModeGT, ModeGTE,
		ModeLT, ModeLTE, ModeBefore, ModeAfter,
		ModeInside, ModeOutside:
		coerced, err := coerceDateTimeFilterValue(f.DataType, f.Value)
		if err != nil {
			return nil, err
		}
		f.Value = coerced
	}
	switch f.Mode {
	case ModeEqual:
		return q.Where("? = ?", col, f.Value), nil
	case ModeNotEqual:
		return q.Where("? != ?", col, f.Value), nil
	case ModeGT:
		return q.Where("? > ?", col, f.Value), nil
	case ModeGTE:
		return q.Where("? >= ?", col, f.Value), nil
	case ModeLT:
		return q.Where("? < ?", col, f.Value), nil
	case ModeLTE:
		return q.Where("? <= ?", col, f.Value), nil
	case ModeBefore:
		return q.Where("? < ?", col, f.Value), nil
	case ModeAfter:
		return q.Where("? > ?", col, f.Value), nil
	case ModeContains:
		return q.Where("? LIKE ?", col, "%"+utils.EscapeLike(fmt.Sprint(f.Value))+"%"), nil
	case ModeNotContains:
		return q.Where("? NOT LIKE ?", col, "%"+utils.EscapeLike(fmt.Sprint(f.Value))+"%"), nil
	case ModeStartsWith:
		return q.Where("? LIKE ?", col, utils.EscapeLike(fmt.Sprint(f.Value))+"%"), nil
	case ModeEndsWith:
		return q.Where("? LIKE ?", col, "%"+utils.EscapeLike(fmt.Sprint(f.Value))), nil
	case ModeInside:
		if reflect.ValueOf(f.Value).Len() == 0 {
			return q.Where("1 = 0"), nil // IN () matches nothing
		}
		return q.Where("? IN (?)", col, bun.List(f.Value)), nil
	case ModeOutside:
		if reflect.ValueOf(f.Value).Len() == 0 {
			return q.Where("1 = 1"), nil // NOT IN () excludes nothing
		}
		return q.Where("? NOT IN (?)", col, bun.List(f.Value)), nil
	case ModeSearch:
		if f.Field != "" {
			return q.Where("? @@@ ?", col, fmt.Sprint(f.Value)), nil
		}
		return q.Where("? @@@ paradedb.parse(?, lenient => true)", bun.Ident(columnDefaultID), fmt.Sprint(f.Value)), nil
	case ModeRange:
		from, to, err := extractRangeBounds(f.Value)
		if err != nil {
			return nil, err
		}
		if from, err = coerceDateTimeFilterValue(f.DataType, from); err != nil {
			return nil, err
		}
		if to, err = coerceDateTimeFilterValue(f.DataType, to); err != nil {
			return nil, err
		}
		return q.Where("? BETWEEN ? AND ?", col, from, to), nil
	case ModeIsEmpty:
		return q.Where("(? IS NULL OR ?::text = '')", col, col), nil
	case ModeIsNotEmpty:
		return q.Where("(? IS NOT NULL AND ?::text != '')", col, col), nil
	default:
		return nil, fmt.Errorf("%w: unsupported mode %q", ErrInvalidFilter, f.Mode)
	}
}

func coerceDateTimeFilterValue(dataType DataType, value any) (any, error) {
	var parse func(string) (time.Time, bool)
	switch dataType {
	case DataTypeDate:
		parse = utils.ParseDateTime
	case DataTypeTime:
		parse = utils.ParseTimeOfDay
	default:
		return value, nil
	}
	switch v := value.(type) {
	case string:
		t, ok := parse(v)
		if !ok {
			return nil, fmt.Errorf("%w: unrecognized %s value %q", ErrInvalidFilter, dataType, v)
		}
		if dataType == DataTypeTime {
			// A time.Time here would be sent as year 0000, which Postgres rejects.
			return t.Format("15:04:05.999999"), nil
		}
		return t, nil
	case []any:
		out := make([]any, len(v))
		for i, elem := range v {
			coerced, err := coerceDateTimeFilterValue(dataType, elem)
			if err != nil {
				return nil, err
			}
			out[i] = coerced
		}
		return out, nil
	default:
		return value, nil
	}
}

func extractRangeBounds(value any) (from, to any, err error) {
	switch v := value.(type) {
	case RangeNumber:
		return v.From, v.To, nil
	case RangeDate:
		return v.From, v.To, nil
	case map[string]any:
		from, okFrom := v["from"]
		to, okTo := v["to"]
		if !okFrom || !okTo {
			return nil, nil, fmt.Errorf("%w: range value missing \"from\"/\"to\": %#v", ErrInvalidFilter, value)
		}
		if from == nil || to == nil {
			return nil, nil, fmt.Errorf("%w: range value's \"from\"/\"to\" must not be null: %#v", ErrInvalidFilter, value)
		}
		return from, to, nil
	default:
		return nil, nil, fmt.Errorf("%w: unsupported range value type %T", ErrInvalidFilter, value)
	}
}
