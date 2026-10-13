package pagination

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/uptrace/bun"
)

func resolveZone(name string) (*time.Location, error) {
	loc, err := utils.ResolveZone(name)
	if err != nil {
		return nil, fmt.Errorf("%w: timezone: %w", ErrInvalidFilter, err)
	}
	return loc, nil
}

func kindOf(sqlType string) colKind {
	t := strings.ToLower(strings.TrimSpace(sqlType))
	switch {
	case t == "date":
		return kindDate
	case strings.HasPrefix(t, "timestamptz"), strings.HasPrefix(t, "timestamp with time zone"):
		return kindTimestampTZ
	case strings.HasPrefix(t, "timestamp"):
		return kindInstant
	case strings.HasPrefix(t, "time"):
		return kindClock
	}
	return kindInstant
}

func (d DateValue) start() time.Time {
	start, _ := utils.DayBounds(d.t)
	return start
}

func (d DateValue) next() time.Time {
	_, next := utils.DayBounds(d.t)
	return next
}

func (d DateValue) calendarDay() string {
	return d.t.Format(utils.DateLayout)
}

func filterErr(err error) error {
	if errors.Is(err, ErrNotDate) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrInvalidFilter, err)
}

func parseDateValue(v any, loc *time.Location) (DateValue, error) {
	t, hasTime, err := utils.ParseDateInput(v, loc)
	if err != nil {
		return DateValue{}, filterErr(err)
	}
	return DateValue{t: t, hasTime: hasTime}, nil
}

func instantCondition(col bun.Ident, mode Mode, t time.Time) (string, []any) {
	f, exact := utils.FloorMicro(t)
	switch mode {
	case ModeEqual:
		if !exact {
			return "1 = 0", nil
		}
		return "? = ?", []any{col, f}
	case ModeNotEqual:
		if !exact {
			return "? IS NOT NULL", []any{col}
		}
		return "? != ?", []any{col, f}
	case ModeGT, ModeAfter:
		return "? > ?", []any{col, f}
	case ModeGTE:
		if exact {
			return "? >= ?", []any{col, f}
		}
		return "? > ?", []any{col, f}
	case ModeLT, ModeBefore:
		if exact {
			return "? < ?", []any{col, f}
		}
		return "? <= ?", []any{col, f}
	default: // ModeLTE
		return "? <= ?", []any{col, f}
	}
}

func (d DateValue) is(col bun.Ident, kind colKind) (string, []any) {
	if d.hasTime {
		return instantCondition(col, ModeEqual, d.t)
	}
	if kind == kindDate {
		return "? = ?", []any{col, d.calendarDay()}
	}
	return "(? >= ? AND ? < ?)", []any{col, d.start(), col, d.next()}
}

func dateCondition(col bun.Ident, f Filter, loc *time.Location, kind colKind) (expr string, args []any, ok bool, err error) {
	switch f.Mode {
	case ModeEqual, ModeNotEqual, ModeGT, ModeGTE, ModeLT, ModeLTE, ModeBefore, ModeAfter:
		d, err := parseDateValue(f.Value, loc)
		if errors.Is(err, ErrNotDate) {
			return "", nil, false, nil
		}
		if err != nil {
			return "", nil, false, err
		}
		expr, args = singleDateCondition(col, f.Mode, d, kind)
		return expr, args, true, nil
	case ModeRange:
		from, to, err := extractRangeBounds(f.Value)
		if err != nil {
			return "", nil, false, err
		}
		lo, errLo := parseDateValue(from, loc)
		hi, errHi := parseDateValue(to, loc)
		for _, e := range []error{errLo, errHi} {
			if errors.Is(e, ErrNotDate) {
				return "", nil, false, nil
			}
			if e != nil {
				return "", nil, false, e
			}
		}
		// Each bound is judged on its own: a bare "from" opens at the start of
		// its day, a bare "to" closes at the end of its day.
		loExpr, loArgs := singleDateCondition(col, ModeGTE, lo, kind)
		hiExpr, hiArgs := singleDateCondition(col, ModeLTE, hi, kind)
		return "(" + loExpr + " AND " + hiExpr + ")", append(loArgs, hiArgs...), true, nil
	case ModeInside, ModeOutside:
		list, _ := utils.AsList(f.Value)
		if len(list) == 0 {
			return "", nil, false, nil
		}
		var parts []string
		for _, item := range list {
			d, err := parseDateValue(item, loc)
			if errors.Is(err, ErrNotDate) {
				return "", nil, false, nil
			}
			if err != nil {
				return "", nil, false, err
			}
			part, partArgs := d.is(col, kind)
			parts = append(parts, part)
			args = append(args, partArgs...)
		}
		expr = "(" + strings.Join(parts, " OR ") + ")"
		if f.Mode == ModeOutside {
			expr = "NOT " + expr
		}
		return expr, args, true, nil
	}
	return "", nil, false, nil
}

func singleDateCondition(col bun.Ident, mode Mode, d DateValue, kind colKind) (string, []any) {
	if d.hasTime {
		return instantCondition(col, mode, d.t)
	}
	if kind == kindDate {
		day := d.calendarDay()
		switch mode {
		case ModeEqual:
			return "? = ?", []any{col, day}
		case ModeNotEqual:
			return "? != ?", []any{col, day}
		case ModeGT, ModeAfter:
			return "? > ?", []any{col, day}
		case ModeGTE:
			return "? >= ?", []any{col, day}
		case ModeLT, ModeBefore:
			return "? < ?", []any{col, day}
		default:
			return "? <= ?", []any{col, day}
		}
	}
	switch mode {
	case ModeEqual:
		return d.is(col, kind)
	case ModeNotEqual:
		return "(? < ? OR ? >= ?)", []any{col, d.start(), col, d.next()}
	case ModeGT, ModeAfter:
		return "? >= ?", []any{col, d.next()}
	case ModeGTE:
		return "? >= ?", []any{col, d.start()}
	case ModeLT, ModeBefore:
		return "? < ?", []any{col, d.start()}
	default: // ModeLTE
		return "? < ?", []any{col, d.next()}
	}
}

// clockValue is a parsed time of day: the clock as written, and the offset it
// was written in, if it named one.

// parseTimeValue reads a time of day from a string in any layout
// utils.ParseTimeOfDay knows, or from a time.Time's clock.
func parseTimeValue(v any) (clockValue, error) {
	lit, off, err := utils.ParseClockInput(v)
	if err != nil {
		return clockValue{}, filterErr(err)
	}
	return clockValue{lit: lit, off: off}, nil
}

// clockExpr is the column's time of day. A timestamptz column is converted
// explicitly, so the answer never depends on the database session's own time
// zone: to the value's own offset when it named one ("17:00:00+08:00" is the
// column's clock at +08:00), otherwise to the filter's zone. A time column has
// no zone and is used as is.
func clockExpr(col bun.Ident, loc *time.Location, kind colKind, off *int) (string, []any) {
	if kind != kindTimestampTZ {
		return "?::time", []any{col}
	}
	if off != nil {
		return "((? AT TIME ZONE 'UTC') + (? * interval '1 second'))::time", []any{col, *off}
	}
	if secs, isOffset := utils.OffsetFromName(loc.String()); isOffset {
		return "((? AT TIME ZONE 'UTC') + (? * interval '1 second'))::time", []any{col, secs}
	}
	if loc == time.UTC {
		return "(? AT TIME ZONE 'UTC')::time", []any{col}
	}
	return "(? AT TIME ZONE ?)::time", []any{col, loc.String()}
}

// timeCondition builds the WHERE term for a DataTypeTime filter. Only the
// time of day is compared, so it works on a time column and on a timestamp
// column alike, whatever its date ("every row created after 09:00"). ok is
// false when the value is not a time at all.
func timeCondition(col bun.Ident, f Filter, loc *time.Location, kind colKind) (expr string, args []any, ok bool, err error) {
	// cmp is "the column's clock <op> the value".
	cmp := func(op string, v clockValue) (string, []any) {
		lhs, lhsArgs := clockExpr(col, loc, kind, v.off)
		return lhs + " " + op + " ?", append(lhsArgs, v.lit)
	}
	parse := func(raw any) (clockValue, bool, error) {
		cv, err := parseTimeValue(raw)
		if errors.Is(err, ErrNotDate) {
			return cv, false, nil
		}
		return cv, err == nil, err
	}
	ops := map[Mode]string{
		ModeEqual: "=", ModeNotEqual: "!=", ModeGT: ">", ModeAfter: ">", ModeGTE: ">=",
		ModeLT: "<", ModeBefore: "<", ModeLTE: "<=",
	}
	switch f.Mode {
	case ModeEqual, ModeNotEqual, ModeGT, ModeAfter, ModeGTE, ModeLT, ModeBefore, ModeLTE:
		cv, ok, err := parse(f.Value)
		if !ok || err != nil {
			return "", nil, false, err
		}
		expr, args = cmp(ops[f.Mode], cv)
		return expr, args, true, nil
	case ModeRange:
		from, to, err := extractRangeBounds(f.Value)
		if err != nil {
			return "", nil, false, err
		}
		lo, okLo, errLo := parse(from)
		hi, okHi, errHi := parse(to)
		if errLo != nil || errHi != nil {
			return "", nil, false, errors.Join(errLo, errHi)
		}
		if !okLo || !okHi {
			return "", nil, false, nil
		}
		loExpr, loArgs := cmp(">=", lo)
		hiExpr, hiArgs := cmp("<=", hi)
		return "(" + loExpr + " AND " + hiExpr + ")", append(loArgs, hiArgs...), true, nil
	case ModeInside, ModeOutside:
		list, _ := utils.AsList(f.Value)
		if len(list) == 0 {
			return "", nil, false, nil
		}
		var parts []string
		for _, item := range list {
			cv, ok, err := parse(item)
			if !ok || err != nil {
				return "", nil, false, err
			}
			part, partArgs := cmp("=", cv)
			parts = append(parts, part)
			args = append(args, partArgs...)
		}
		expr = "(" + strings.Join(parts, " OR ") + ")"
		if f.Mode == ModeOutside {
			expr = "NOT " + expr
		}
		return expr, args, true, nil
	}
	return "", nil, false, nil
}
