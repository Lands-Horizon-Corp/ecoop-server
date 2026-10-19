package utils

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // zone names must resolve on a host that ships no tzdata
)

var (
	// ErrInvalidZone is returned for a time zone the library will not guess at.
	ErrInvalidZone = errors.New("invalid time zone")
	// ErrNotDate marks a value that is not a date or time at all (a number, a
	// bool...), as opposed to a date or time that is malformed.
	ErrNotDate = errors.New("not a date value")
	// ErrInvalidDate is a string no known date or time layout reads.
	ErrInvalidDate = errors.New("unrecognized date or time")
	// ErrAmbiguousZone is a value that names its zone by abbreviation (EST...).
	ErrAmbiguousZone = errors.New("time zone abbreviation is ambiguous; use an offset such as +02:00 or Z")
)

// DateLayout formats a calendar date.
const DateLayout = "2006-01-02"

// ClockLayout formats a time of day, keeping sub-second digits when there are
// some and dropping them when there are not.
const ClockLayout = "15:04:05.999999"

var offsetZone = regexp.MustCompile(`^([+-])(\d{2}):?(\d{2})?$`)

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

var dateTimeLayouts = []string{
	time.RFC3339,
	time.RFC3339Nano,
	time.RFC1123,
	time.RFC1123Z,
	time.RFC822,
	time.RFC822Z,
	time.RFC850,
	time.ANSIC,
	time.UnixDate,
	time.RubyDate,
	"2006-01-02T15:04:05Z",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999999",
	"01/02/2006 15:04:05",
	"02/01/2006 15:04:05",
	"2006-01-02T15:04:05-07:00",
	"Mon Jan 02 2006 15:04:05 -0700",
	"2006/01/02 15:04:05",
	"2006/01/02T15:04:05",
	"2006/01/02 15:04:05Z07:00",
	"2006/01/02 15:04:05 MST",
	"2006-01-02",
	"2006/01/02",
	"01/02/2006",
	"02/01/2006",
}

var timeLayouts = []string{
	time.Kitchen,
	"15:04:05",
	"15:04",
	"15:04:05.999999999",
	"3:04:05 PM",
	"3:04 PM",
	"15:04:05Z07:00",
	"15:04:05 MST",
	"3:04:05 PM MST",
	"15:04:05-07:00",
}

// ParseDateTime reads a date and time in any known layout. A value without a
// zone is read as UTC.
func ParseDateTime(s string) (t time.Time, ok bool) {
	return ParseDateTimeIn(s, time.UTC)
}

// ParseDateTimeIn is ParseDateTime for values without a zone: they are read in
// loc. A zone in the value itself always wins.
func ParseDateTimeIn(s string, loc *time.Location) (t time.Time, ok bool) {
	for _, layout := range dateTimeLayouts {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func ParseTimeOfDay(s string) (t time.Time, ok bool) {
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// ParseDateOnly reads a bare date (no time of day) in any of the accepted
// layouts, as midnight in loc.
func ParseDateOnly(s string, loc *time.Location) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range dateOnlyLayouts {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// DayBounds returns the first instant of t's day in its own zone and the first
// instant of the next day. The span is half-open, [start, next), and is 23 or
// 25 hours long on a daylight-saving change.
func DayBounds(t time.Time) (start, next time.Time) {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location()), time.Date(y, m, d+1, 0, 0, 0, 0, t.Location())
}

// FloorMicro floors an instant to the microsecond Postgres stores, and says
// whether that lost anything.
func FloorMicro(t time.Time) (floor time.Time, exact bool) {
	floor = t.Truncate(time.Microsecond)
	return floor, floor.Equal(t)
}

// OffsetFromName reads "+08:00", "-0330" or "+05" as seconds east of UTC.
func OffsetFromName(name string) (int, bool) {
	m := offsetZone.FindStringSubmatch(strings.TrimSpace(name))
	if m == nil {
		return 0, false
	}
	h, _ := strconv.Atoi(m[2])
	mins, _ := strconv.Atoi(m[3])
	if mins > 59 {
		return 0, false
	}
	secs := h*3600 + mins*60
	if m[1] == "-" {
		secs = -secs
	}
	return secs, true
}

// ResolveZone turns a zone name into a location: an IANA name (Area/City), an
// offset such as +08:00, or UTC. Empty is UTC. "Local" is refused because it is
// whatever the server happens to be set to, and so are single-word names (EST,
// Japan...), which are abbreviations or nicknames.
func ResolveZone(name string) (*time.Location, error) {
	name = strings.TrimSpace(name)
	switch strings.ToUpper(name) {
	case "", "UTC", "Z", "GMT":
		return time.UTC, nil
	case "LOCAL":
		return nil, fmt.Errorf("%w: %q is the server's own zone; name one", ErrInvalidZone, name)
	}
	if secs, ok := OffsetFromName(name); ok {
		if secs > 14*3600 || secs < -12*3600 {
			return nil, fmt.Errorf("%w: offset %q is out of range", ErrInvalidZone, name)
		}
		return time.FixedZone(name, secs), nil
	}
	if !strings.Contains(name, "/") {
		return nil, fmt.Errorf("%w: %q is ambiguous; use a name like Asia/Manila or an offset like +08:00", ErrInvalidZone, name)
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("%w: unknown zone %q", ErrInvalidZone, name)
	}
	return loc, nil
}

// IsZoneAbbreviation reports whether a value ends in a zone abbreviation (EST,
// PST, IST...). Go resolves an unknown abbreviation to UTC+0 and a known one
// through the server's own zone, so the result would depend on where the
// server runs. Numeric offsets, Z, UTC and GMT are unambiguous. (The parsed
// location's name cannot be used: Go names a numeric offset "+0000", or
// "Local" when it matches the server's zone.)
func IsZoneAbbreviation(s string) bool {
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

// HasExplicitZone reports whether a time-of-day string names its own zone: Z,
// a numeric offset, UTC or GMT.
func HasExplicitZone(s string) bool {
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

// IsMidnight reports whether t is exactly 00:00:00.000000000 on its day.
func IsMidnight(t time.Time) bool {
	return t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0
}

// AsList returns the items of a slice or array value, or false if v is
// anything else (including nil).
func AsList(v any) ([]any, bool) {
	if v == nil {
		return nil, false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := range out {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

// ParseDateInput reads a date from a client-supplied value: a string in any
// known layout, or a time.Time. hasTime tells a bare date ("2024-03-01", a
// whole day, returned as midnight in loc) from an instant ("2024-03-01T09:00:00Z",
// that moment). A string without a zone is read in loc; one with an offset keeps
// it; one naming its zone by abbreviation is refused. A value of any other type
// is ErrNotDate.
func ParseDateInput(v any, loc *time.Location) (t time.Time, hasTime bool, err error) {
	switch x := v.(type) {
	case string:
		x = strings.TrimSpace(x)
		if t, ok := ParseDateOnly(x, loc); ok {
			return t, false, nil
		}
		if t, ok := ParseDateTimeIn(x, loc); ok {
			if IsZoneAbbreviation(x) {
				return time.Time{}, false, fmt.Errorf("%w: %q", ErrAmbiguousZone, x)
			}
			return t, true, nil
		}
		return time.Time{}, false, fmt.Errorf("%w: %q", ErrInvalidDate, x)
	case time.Time:
		if IsMidnight(x) {
			// Midnight names a day: the one it falls on, taken in loc.
			return time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, loc), false, nil
		}
		return x, true, nil
	}
	return time.Time{}, false, ErrNotDate
}

// ParseClockInput reads a time of day from a client-supplied value: a string in
// any layout ParseTimeOfDay knows (am/pm in any case), or a time.Time. lit is
// the clock as written, formatted with ClockLayout; off is the offset it was
// written in (seconds east of UTC), or nil if it named none. A value of any
// other type is ErrNotDate.
func ParseClockInput(v any) (lit string, off *int, err error) {
	switch x := v.(type) {
	case string:
		x = strings.ToUpper(strings.TrimSpace(x)) // "6:00 pm" is "6:00 PM"
		t, ok := ParseTimeOfDay(x)
		if !ok {
			return "", nil, fmt.Errorf("%w: %q", ErrInvalidDate, x)
		}
		if IsZoneAbbreviation(x) {
			return "", nil, fmt.Errorf("%w: %q", ErrAmbiguousZone, x)
		}
		if HasExplicitZone(x) {
			_, o := t.Zone()
			off = &o
		}
		return t.Format(ClockLayout), off, nil
	case time.Time:
		_, o := x.Zone()
		return x.Format(ClockLayout), &o, nil
	}
	return "", nil, ErrNotDate
}
