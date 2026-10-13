package pagination

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // zone names must resolve on a host that ships no tzdata

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/uptrace/bun"
)

// Dates and times in a filter are read in a zone and compared in UTC, the zone
// the database stores. A value carrying its own offset ("...T09:00:00+08:00")
// is an exact instant. A bare date ("2024-03-10") is a whole day of the
// filter's Timezone: from that zone's midnight up to, not including, its next
// midnight, so a ledger for "March 10 in Manila" is exactly what Manila
// experienced that day, 23 or 25 hours long on a daylight-saving change.

// errNotDate marks a value dateCondition does not interpret (a number, a
// bool...); the caller falls back to comparing it as given.
var errNotDate = errors.New("not a date value")

// dateOnlyLayouts are the layouts that name a day and nothing else. Month and
// day may be one or two digits. Order resolves the ambiguous ones:
//
//   - year first (any of - / .) is year-month-day;
//   - with slashes and the year last, month comes first (US: 03/04/2024 is
//     March 4th), and only when that is impossible (13/01/2024) day first;
//   - with dots or hyphens and the year last, day comes first (01.03.2024 and
//     01-03-2024 are March 1st).
//
// Two-digit years are refused: the century would be a guess.
var dateOnlyLayouts = []string{
	"2006-1-2", "2006/1/2", "2006.1.2",
	"1/2/2006", "2/1/2006",
	"2.1.2006", "2-1-2006",
}

var offsetZone = regexp.MustCompile(`^([+-])(\d{2}):?(\d{2})?$`)

// resolveZone turns a Timezone into a location: an IANA name, an offset such
// as +08:00, or UTC. Empty is UTC. "Local" is refused because it is whatever
// the server happens to be set to.
func resolveZone(name string) (*time.Location, error) {
	name = strings.TrimSpace(name)
	switch strings.ToUpper(name) {
	case "", "UTC", "Z", "GMT":
		return time.UTC, nil
	case "LOCAL":
		return nil, fmt.Errorf("%w: timezone %q is the server's own zone; name one", ErrInvalidFilter, name)
	}
	if secs, ok := offsetFromName(name); ok {
		if secs > 14*3600 || secs < -12*3600 {
			return nil, fmt.Errorf("%w: timezone offset %q is out of range", ErrInvalidFilter, name)
		}
		return time.FixedZone(name, secs), nil
	}
	// Legacy single-word names (EST, MST, Japan...) are abbreviations or country
	// nicknames; the unambiguous spelling is Area/City.
	if !strings.Contains(name, "/") {
		return nil, fmt.Errorf("%w: timezone %q is ambiguous; use a name like Asia/Manila or an offset like +08:00", ErrInvalidFilter, name)
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("%w: unknown timezone %q", ErrInvalidFilter, name)
	}
	return loc, nil
}

// offsetFromName reads "+08:00", "-0330" or "+05" as seconds east of UTC.
func offsetFromName(name string) (int, bool) {
	m := offsetZone.FindStringSubmatch(strings.TrimSpace(name))
	if m == nil {
		return 0, false
	}
	h, _ := strconv.Atoi(m[2])
	min, _ := strconv.Atoi(m[3])
	if min > 59 {
		return 0, false
	}
	secs := h*3600 + min*60
	if m[1] == "-" {
		secs = -secs
	}
	return secs, true
}

// zoneIsAbbreviation reports whether a value ends in a zone abbreviation (EST,
// PST, IST...). Go resolves an unknown abbreviation to UTC+0 and a known one
// through the server's own zone, so the result would depend on where the
// server runs. Numeric offsets, Z, UTC and GMT are unambiguous. (The parsed
// location's name cannot be used: Go names a numeric offset "+0000", or
// "Local" when it matches the server's zone.)
func zoneIsAbbreviation(s string) bool {
	fields := strings.Fields(s)
	if len(fields) < 2 {
		return false
	}
	tail := strings.ToUpper(fields[len(fields)-1])
	switch tail {
	case "UTC", "GMT", "AM", "PM":
		return false
	}
	if len(tail) < 2 || len(tail) > 5 {
		return false
	}
	for _, r := range tail {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

// colKind is how a column stores time.
type colKind int

const (
	kindInstant     colKind = iota // unknown or timestamp: compared as instants
	kindDate                       // date: a calendar day with no zone
	kindTimestampTZ                // timestamptz
	kindClock                      // time, timetz: a time of day with no date
)

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

// dateValue is a parsed filter value. hasTime tells a bare date ("2024-03-01",
// the whole day) from an instant ("2024-03-01T00:00:00Z", that moment).
type dateValue struct {
	t       time.Time
	hasTime bool
}

// start is the first instant of the value's day in its zone; next is the first
// instant of the following day. Day bounds are half-open ([start, next)): an
// inclusive "23:59:59.999999999" end would be one nanosecond short of a
// microsecond-stored row and one microsecond long of a millisecond one.
func (d dateValue) start() time.Time {
	return time.Date(d.t.Year(), d.t.Month(), d.t.Day(), 0, 0, 0, 0, d.t.Location())
}

func (d dateValue) next() time.Time {
	return time.Date(d.t.Year(), d.t.Month(), d.t.Day()+1, 0, 0, 0, 0, d.t.Location())
}

// calendarDay is the value's date as a plain literal, for a date column.
func (d dateValue) calendarDay() string { return d.t.Format("2006-01-02") }

func parseDateValue(v any, loc *time.Location) (dateValue, error) {
	switch x := v.(type) {
	case string:
		x = strings.TrimSpace(x)
		for _, layout := range dateOnlyLayouts {
			if t, err := time.ParseInLocation(layout, x, loc); err == nil {
				return dateValue{t: t}, nil
			}
		}
		if t, ok := utils.ParseDateTimeIn(x, loc); ok {
			if zoneIsAbbreviation(x) {
				return dateValue{}, fmt.Errorf("%w: %q names its time zone by abbreviation, which is ambiguous; use an offset such as +02:00 or Z", ErrInvalidFilter, x)
			}
			return dateValue{t: t, hasTime: true}, nil
		}
		return dateValue{}, fmt.Errorf("%w: unrecognized %s value %q", ErrInvalidFilter, DataTypeDate, x)
	case time.Time:
		if x.Hour() == 0 && x.Minute() == 0 && x.Second() == 0 && x.Nanosecond() == 0 {
			// Midnight names a day: the one it falls on, taken in the filter's zone.
			return dateValue{t: time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, loc)}, nil
		}
		return dateValue{t: x, hasTime: true}, nil
	}
	return dateValue{}, errNotDate
}

// micro floors an instant to the microsecond Postgres stores, and says whether
// that lost anything.
func micro(t time.Time) (floor time.Time, exact bool) {
	floor = t.Truncate(time.Microsecond)
	return floor, floor.Equal(t)
}

// instantCondition compares a column with an exact instant. Finer than a
// microsecond, an instant lies between two storable values: "equal" can match
// nothing, ">=" starts at the next microsecond and "<" ends at the floor.
func instantCondition(col bun.Ident, mode Mode, t time.Time) (string, []any) {
	f, exact := micro(t)
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

// is returns the condition that holds when col is on the value's day (a bare
// date) or at its exact instant.
func (d dateValue) is(col bun.Ident, kind colKind) (string, []any) {
	if d.hasTime {
		return instantCondition(col, ModeEqual, d.t)
	}
	if kind == kindDate {
		return "? = ?", []any{col, d.calendarDay()}
	}
	return "(? >= ? AND ? < ?)", []any{col, d.start(), col, d.next()}
}

// dateCondition builds the WHERE term for a DataTypeDate filter. A bare date
// means the whole day of the filter's zone, so "= 2024-03-01" matches every
// time on that day, ">" matches from the next day on and "<=" up to the end of
// that day. A value with a time of day is an exact instant. On a date column,
// which has no zone, a bare date is compared as the calendar day itself. ok is
// false when the value is not a date at all, and the caller compares it
// unchanged.
func dateCondition(col bun.Ident, f Filter, loc *time.Location, kind colKind) (expr string, args []any, ok bool, err error) {
	switch f.Mode {
	case ModeEqual, ModeNotEqual, ModeGT, ModeGTE, ModeLT, ModeLTE, ModeBefore, ModeAfter:
		d, err := parseDateValue(f.Value, loc)
		if errors.Is(err, errNotDate) {
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
			if errors.Is(e, errNotDate) {
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
		list := reflect.ValueOf(f.Value)
		if list.Len() == 0 {
			return "", nil, false, nil
		}
		var parts []string
		for i := range list.Len() {
			d, err := parseDateValue(list.Index(i).Interface(), loc)
			if errors.Is(err, errNotDate) {
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

func singleDateCondition(col bun.Ident, mode Mode, d dateValue, kind colKind) (string, []any) {
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

// timeLayout keeps sub-second digits when a value has them and drops them
// when it does not.
const timeLayout = "15:04:05.999999"

// clockValue is a parsed time of day: the clock as written, and the offset it
// was written in, if it named one.
type clockValue struct {
	lit string
	off *int // seconds east of UTC; nil when the value names no zone
}

// parseTimeValue reads a time of day from a string in any layout
// utils.ParseTimeOfDay knows, or from a time.Time's clock.
func parseTimeValue(v any) (clockValue, error) {
	switch x := v.(type) {
	case string:
		x = strings.ToUpper(strings.TrimSpace(x)) // "6:00 pm" is "6:00 PM"
		t, ok := utils.ParseTimeOfDay(x)
		if !ok {
			return clockValue{}, fmt.Errorf("%w: unrecognized %s value %q", ErrInvalidFilter, DataTypeTime, x)
		}
		if zoneIsAbbreviation(x) {
			return clockValue{}, fmt.Errorf("%w: %q names its time zone by abbreviation, which is ambiguous; use an offset such as +02:00 or Z", ErrInvalidFilter, x)
		}
		cv := clockValue{lit: t.Format(timeLayout)}
		if hasExplicitZone(x) {
			_, off := t.Zone()
			cv.off = &off
		}
		return cv, nil
	case time.Time:
		_, off := x.Zone()
		return clockValue{lit: x.Format(timeLayout), off: &off}, nil
	}
	return clockValue{}, errNotDate
}

// hasExplicitZone reports whether a time-of-day string names its own zone.
func hasExplicitZone(s string) bool {
	if strings.ContainsAny(s, "Z+-") {
		return true
	}
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return false
	}
	tail := fields[len(fields)-1]
	return tail == "UTC" || tail == "GMT"
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
	if secs, isOffset := offsetFromName(loc.String()); isOffset {
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
		if errors.Is(err, errNotDate) {
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
		list := reflect.ValueOf(f.Value)
		if list.Len() == 0 {
			return "", nil, false, nil
		}
		var parts []string
		for i := range list.Len() {
			cv, ok, err := parse(list.Index(i).Interface())
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
