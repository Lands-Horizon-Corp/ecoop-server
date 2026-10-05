package pagination

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) applyFilters(
	q *bun.SelectQuery, filterRoot database.StructuredFilter,
) (*bun.SelectQuery, error) {
	if len(filterRoot.Filters) == 0 {
		return q, nil
	}
	sep := "AND"
	if filterRoot.Logic == database.LogicOr {
		sep = "OR"
	}
	var groupErr error
	q = q.WhereGroup("AND", func(q *bun.SelectQuery) *bun.SelectQuery {
		for _, f := range filterRoot.Filters {
			if groupErr != nil {
				break
			}
			wholeIndexSearch := f.Mode == database.ModeSearch && f.Field == ""
			isCustom := f.Mode == database.ModeCustom
			if !wholeIndexSearch && !isCustom && utils.BunColumnFieldIndex[TData](f.Field) == -1 {
				groupErr = fmt.Errorf("unknown filter field %q", f.Field)
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

func (c *PaginationService[TData, TID]) applyTerm(q *bun.SelectQuery, f database.Filter) (*bun.SelectQuery, error) {
	if f.Mode != database.ModeCustom {
		return applyFilterTerm(q, f, c.ColumnDefaultID)
	}
	if f.Custom == nil {
		return nil, fmt.Errorf("ModeCustom requires Filter.Custom")
	}
	built, err := f.Custom(q, f.Value)
	if err != nil {
		return nil, err
	}
	if built == nil {
		return nil, fmt.Errorf("custom filter returned a nil query")
	}
	return built, nil
}

func applyFilterTerm(q *bun.SelectQuery, f database.Filter, columnDefaultID string) (*bun.SelectQuery, error) {
	col := bun.Ident(f.Field)
	switch f.Mode {
	case database.ModeEqual, database.ModeNotEqual, database.ModeGT, database.ModeGTE,
		database.ModeLT, database.ModeLTE, database.ModeBefore, database.ModeAfter,
		database.ModeContains, database.ModeNotContains, database.ModeStartsWith, database.ModeEndsWith,
		database.ModeSearch, database.ModeRange:
		if f.Value == nil {
			return nil, fmt.Errorf(
				"mode %q requires a non-nil value (use ModeIsEmpty/ModeIsNotEmpty to match null/empty values instead)",
				f.Mode,
			)
		}
	case database.ModeInside, database.ModeOutside:
		if f.Value == nil {
			return nil, fmt.Errorf("mode %q requires a non-nil list value", f.Mode)
		}
		if k := reflect.ValueOf(f.Value).Kind(); k != reflect.Slice && k != reflect.Array {
			return nil, fmt.Errorf("mode %q requires a list value, got %T", f.Mode, f.Value)
		}
	}
	switch f.Mode {
	case database.ModeEqual, database.ModeNotEqual, database.ModeGT, database.ModeGTE,
		database.ModeLT, database.ModeLTE, database.ModeBefore, database.ModeAfter,
		database.ModeInside, database.ModeOutside:
		coerced, err := coerceDateTimeFilterValue(f.DataType, f.Value)
		if err != nil {
			return nil, err
		}
		f.Value = coerced
	}
	switch f.Mode {
	case database.ModeEqual:
		return q.Where("? = ?", col, f.Value), nil
	case database.ModeNotEqual:
		return q.Where("? != ?", col, f.Value), nil
	case database.ModeGT:
		return q.Where("? > ?", col, f.Value), nil
	case database.ModeGTE:
		return q.Where("? >= ?", col, f.Value), nil
	case database.ModeLT:
		return q.Where("? < ?", col, f.Value), nil
	case database.ModeLTE:
		return q.Where("? <= ?", col, f.Value), nil
	case database.ModeBefore:
		return q.Where("? < ?", col, f.Value), nil
	case database.ModeAfter:
		return q.Where("? > ?", col, f.Value), nil
	case database.ModeContains:
		return q.Where("? LIKE ?", col, "%"+escapeLike(fmt.Sprint(f.Value))+"%"), nil
	case database.ModeNotContains:
		return q.Where("? NOT LIKE ?", col, "%"+escapeLike(fmt.Sprint(f.Value))+"%"), nil
	case database.ModeStartsWith:
		return q.Where("? LIKE ?", col, escapeLike(fmt.Sprint(f.Value))+"%"), nil
	case database.ModeEndsWith:
		return q.Where("? LIKE ?", col, "%"+escapeLike(fmt.Sprint(f.Value))), nil
	case database.ModeInside:
		return q.Where("? IN (?)", col, bun.List(f.Value)), nil
	case database.ModeOutside:
		return q.Where("? NOT IN (?)", col, bun.List(f.Value)), nil
	case database.ModeSearch:
		if f.Field != "" {
			return q.Where("? @@@ ?", col, fmt.Sprint(f.Value)), nil
		}
		return q.Where("? @@@ paradedb.parse(?, lenient => true)", bun.Ident(columnDefaultID), fmt.Sprint(f.Value)), nil
	case database.ModeRange:
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
	case database.ModeIsEmpty:
		return q.Where("(? IS NULL OR ? = '')", col, col), nil
	case database.ModeIsNotEmpty:
		return q.Where("(? IS NOT NULL AND ? != '')", col, col), nil
	default:
		return nil, fmt.Errorf("unsupported mode %q", f.Mode)
	}
}

func coerceDateTimeFilterValue(dataType database.DataType, value any) (any, error) {
	var parse func(string) (time.Time, bool)
	switch dataType {
	case database.DataTypeDate:
		parse = utils.ParseDateTime
	case database.DataTypeTime:
		parse = utils.ParseTimeOfDay
	default:
		return value, nil
	}
	switch v := value.(type) {
	case string:
		t, ok := parse(v)
		if !ok {
			return nil, fmt.Errorf("unrecognized %s value %q", dataType, v)
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
	case database.RangeNumber:
		return v.From, v.To, nil
	case database.RangeDate:
		return v.From, v.To, nil
	case map[string]any:
		from, okFrom := v["from"]
		to, okTo := v["to"]
		if !okFrom || !okTo {
			return nil, nil, fmt.Errorf("range value missing \"from\"/\"to\": %#v", value)
		}
		if from == nil || to == nil {
			return nil, nil, fmt.Errorf("range value's \"from\"/\"to\" must not be null: %#v", value)
		}
		return from, to, nil
	default:
		return nil, nil, fmt.Errorf("unsupported range value type %T", value)
	}
}
