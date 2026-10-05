package pagination

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
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
			wholeIndexSearch := f.Mode == ModeSearch && f.Field == ""
			isCustom := f.Mode == ModeCustom
			if !wholeIndexSearch && !isCustom && utils.BunColumnFieldIndex[TData](f.Field) == -1 {
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
		return applyFilterTerm(q, f, c.ColumnDefaultID)
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

func applyFilterTerm(q *bun.SelectQuery, f Filter, columnDefaultID string) (*bun.SelectQuery, error) {
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
		return q.Where("? LIKE ?", col, "%"+escapeLike(fmt.Sprint(f.Value))+"%"), nil
	case ModeNotContains:
		return q.Where("? NOT LIKE ?", col, "%"+escapeLike(fmt.Sprint(f.Value))+"%"), nil
	case ModeStartsWith:
		return q.Where("? LIKE ?", col, escapeLike(fmt.Sprint(f.Value))+"%"), nil
	case ModeEndsWith:
		return q.Where("? LIKE ?", col, "%"+escapeLike(fmt.Sprint(f.Value))), nil
	case ModeInside:
		return q.Where("? IN (?)", col, bun.List(f.Value)), nil
	case ModeOutside:
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
		return q.Where("(? IS NULL OR ? = '')", col, col), nil
	case ModeIsNotEmpty:
		return q.Where("(? IS NOT NULL AND ? != '')", col, col), nil
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

func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
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
