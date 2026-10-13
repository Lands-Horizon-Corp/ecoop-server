//go:build integration

package pagination_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

// A bare date filters by the whole day, whatever time the stored value has;
// a value with a time of day is an exact instant. These run against the
// timestamptz column "at", where the difference matters.

func dayWide(mode pagination.Mode, at, ref time.Time) bool {
	start := time.Date(ref.Year(), ref.Month(), ref.Day(), 0, 0, 0, 0, time.UTC)
	next := start.AddDate(0, 0, 1)
	switch mode {
	case pagination.ModeEqual:
		return !at.Before(start) && at.Before(next)
	case pagination.ModeNotEqual:
		return at.Before(start) || !at.Before(next)
	case pagination.ModeGT, pagination.ModeAfter:
		return !at.Before(next)
	case pagination.ModeGTE:
		return !at.Before(start)
	case pagination.ModeLT, pagination.ModeBefore:
		return at.Before(start)
	case pagination.ModeLTE:
		return at.Before(next)
	}
	panic("no day-wide oracle for " + string(mode))
}

var dateOnlyProbes = []struct{ sent, day string }{
	{"2023-12-31", "2023-12-31"},
	{"2024-01-01", "2024-01-01"},
	{"2024-01-15", "2024-01-15"}, // stored at 12:30, not midnight
	{"2024-01-31", "2024-01-31"}, // stored at 23:59:59
	{"2024-02-29", "2024-02-29"}, // leap day; the next row is the next midnight
	{"2024-03-01", "2024-03-01"},
	{"2024-03-10", "2024-03-10"},
	{"2024-06-30", "2024-06-30"},
	{"2024-07-04", "2024-07-04"},
	{"2024-10-31", "2024-10-31"},
	{"2024-12-31", "2024-12-31"},
	{"2025-01-01", "2025-01-01"},
	{"1999-01-01", "1999-01-01"},
	{"2100-01-01", "2100-01-01"},
	{"2024/03/10", "2024-03-10"},
	{"03/10/2024", "2024-03-10"},
}

func TestMatrix_WholeDayComparisons(t *testing.T) {
	fx := needEvents(t)
	for _, mode := range []pagination.Mode{
		pagination.ModeEqual, pagination.ModeNotEqual, pagination.ModeGT, pagination.ModeGTE,
		pagination.ModeLT, pagination.ModeLTE, pagination.ModeBefore, pagination.ModeAfter,
	} {
		t.Run(string(mode), func(t *testing.T) {
			for _, p := range dateOnlyProbes {
				t.Run(p.sent, func(t *testing.T) {
					t.Parallel()
					ref := day(p.day)
					flt := pagination.Filter{Field: "at", Mode: mode, Value: p.sent, DataType: pagination.DataTypeDate}
					checkEvents(t, fx, flt, func(e Event) bool { return dayWide(mode, e.At, ref) })
				})
			}
		})
	}
}

// The same value with and without a time of day filters differently.
func TestMatrix_BareDateVersusInstant(t *testing.T) {
	fx := needEvents(t)
	at := func(mode pagination.Mode, v string) pagination.Filter {
		return pagination.Filter{Field: "at", Mode: mode, Value: v, DataType: pagination.DataTypeDate}
	}
	t.Run("equal a bare day matches the 12:30 row, midnight does not", func(t *testing.T) {
		t.Parallel()
		checkEvents(t, fx, at(pagination.ModeEqual, "2024-01-15"), func(e Event) bool { return e.Title == "mid-jan" })
		checkEvents(t, fx, at(pagination.ModeEqual, "2024-01-15T00:00:00Z"), func(Event) bool { return false })
	})
	t.Run("after a bare day skips that whole day", func(t *testing.T) {
		t.Parallel()
		checkEvents(t, fx, at(pagination.ModeAfter, "2024-01-31"), func(e Event) bool { return e.At.After(instant("2024-01-31T23:59:59Z")) })
	})
	t.Run("after an instant is strict", func(t *testing.T) {
		t.Parallel()
		checkEvents(t, fx, at(pagination.ModeAfter, "2024-01-31T23:59:59Z"), func(e Event) bool { return e.At.After(instant("2024-01-31T23:59:59Z")) })
	})
	t.Run("lte a bare day includes its last second but not the next midnight", func(t *testing.T) {
		t.Parallel()
		checkEvents(t, fx, at(pagination.ModeLTE, "2024-02-29"), func(e Event) bool { return e.At.Before(instant("2024-03-01T00:00:00Z")) })
	})
	t.Run("lte an instant stops at the instant", func(t *testing.T) {
		t.Parallel()
		checkEvents(t, fx, at(pagination.ModeLTE, "2024-02-29T07:59:59Z"), func(e Event) bool { return !e.At.After(instant("2024-02-29T07:59:59Z")) })
	})
	t.Run("before a bare day excludes that day", func(t *testing.T) {
		t.Parallel()
		checkEvents(t, fx, at(pagination.ModeBefore, "2024-01-01"), func(e Event) bool { return e.Title == "new-year-eve" })
	})
	t.Run("notEqual a bare day removes the whole day", func(t *testing.T) {
		t.Parallel()
		checkEvents(t, fx, at(pagination.ModeNotEqual, "2024-02-29"), func(e Event) bool { return e.Title != "leap-day" })
	})
}

// Each bound of a range is judged on its own.
func TestMatrix_RangeBoundsMixDatesAndInstants(t *testing.T) {
	fx := needEvents(t)
	cases := []struct {
		name     string
		from, to any
		lo, hi   time.Time // inclusive lo, inclusive hi
	}{
		{"bare both", "2024-01-15", "2024-01-31", instant("2024-01-15T00:00:00Z"), instant("2024-01-31T23:59:59.999999Z")},
		{"bare from, timed to", "2024-01-15", "2024-01-31T12:00:00Z", instant("2024-01-15T00:00:00Z"), instant("2024-01-31T12:00:00Z")},
		{"timed from, bare to", "2024-01-15T13:00:00Z", "2024-01-31", instant("2024-01-15T13:00:00Z"), instant("2024-01-31T23:59:59.999999Z")},
		{"timed both", "2024-01-15T12:30:00Z", "2024-01-31T23:59:59Z", instant("2024-01-15T12:30:00Z"), instant("2024-01-31T23:59:59Z")},
		{"one bare day", "2024-01-31", "2024-01-31", instant("2024-01-31T00:00:00Z"), instant("2024-01-31T23:59:59.999999Z")},
		{"leap into march", "2024-02-29", "2024-03-01", instant("2024-02-29T00:00:00Z"), instant("2024-03-01T23:59:59.999999Z")},
		{"to stops before the next midnight", "2024-02-01", "2024-02-29", instant("2024-02-01T00:00:00Z"), instant("2024-02-29T23:59:59.999999Z")},
		{"across the new year", "2023-12-31", "2024-01-01", instant("2023-12-31T00:00:00Z"), instant("2024-01-01T23:59:59.999999Z")},
		{"slash layouts", "2024/06/30", "07/04/2024", instant("2024-06-30T00:00:00Z"), instant("2024-07-04T23:59:59.999999Z")},
		{"nothing inside", "2024-03-02", "2024-03-09", instant("2024-03-02T00:00:00Z"), instant("2024-03-09T23:59:59.999999Z")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			flt := pagination.Filter{Field: "at", Mode: pagination.ModeRange, DataType: pagination.DataTypeDate, Value: map[string]any{"from": tc.from, "to": tc.to}}
			checkEvents(t, fx, flt, func(e Event) bool { return !e.At.Before(tc.lo) && !e.At.After(tc.hi) })
		})
	}
	t.Run("typed range at midnight means whole days", func(t *testing.T) {
		t.Parallel()
		flt := pagination.Filter{Field: "at", Mode: pagination.ModeRange, DataType: pagination.DataTypeDate,
			Value: pagination.RangeDate{From: instant("2024-01-31T00:00:00Z"), To: instant("2024-01-31T00:00:00Z")}}
		checkEvents(t, fx, flt, func(e Event) bool { return e.Title == "end-jan" })
	})
}

// In a list a bare date is a whole day and a timed value is that instant.
func TestMatrix_ListsMixDatesAndInstants(t *testing.T) {
	fx := needEvents(t)
	cases := []struct {
		name  string
		list  []any
		match func(Event) bool
	}{
		{"bare days", []any{"2024-01-15", "2024-02-29"}, func(e Event) bool { return e.Title == "mid-jan" || e.Title == "leap-day" }},
		{"bare day and exact instant", []any{"2024-01-15", "2024-01-31T23:59:59Z"}, func(e Event) bool { return e.Title == "mid-jan" || e.Title == "end-jan" }},
		{"instant that misses", []any{"2024-01-31T23:59:58Z"}, func(Event) bool { return false }},
		{"same day twice", []any{"2024-03-01", "03/01/2024"}, func(e Event) bool { return e.Title == "march-1" }},
		{"slash and ISO", []any{"2024/03/10", "2024-12-31"}, func(e Event) bool { return e.Title == "us-dst" || e.Title == "dec-31" }},
		{"no match", []any{"1999-01-01"}, func(Event) bool { return false }},
	}
	for _, mode := range []pagination.Mode{pagination.ModeInside, pagination.ModeOutside} {
		for _, tc := range cases {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				flt := pagination.Filter{Field: "at", Mode: mode, DataType: pagination.DataTypeDate, Value: tc.list}
				checkEvents(t, fx, flt, func(e Event) bool { return tc.match(e) == (mode == pagination.ModeInside) })
			})
		}
	}
}

// DataTypeTime compares only the time of day, so it can filter a timestamp
// column by "after 09:00" whatever the date.
func atClock(e Event) time.Time { return clock(e.At.UTC().Format("15:04:05")) }

var timeOfDayProbes = []string{"00:00:00", "00:00:01", "07:00:00", "08:00:00", "09:15:30", "12:30", "18:45:00", "21:00:00", "23:59:58", "23:59:59", "9:15 AM", "6:45 PM", "11:59 PM", "05:00"}

func TestMatrix_TimeOfDayOnTimestampColumn(t *testing.T) {
	fx := needEvents(t)
	var tz string
	if err := fx.sqlDB().NewRaw("SHOW timezone").Scan(ctxT(t), &tz); err != nil {
		t.Fatal(err)
	}
	if tz != "UTC" && tz != "Etc/UTC" {
		t.Skipf("session time zone is %s; timestamptz::time follows it, and the oracle assumes UTC", tz)
	}
	for _, mode := range []pagination.Mode{
		pagination.ModeEqual, pagination.ModeNotEqual, pagination.ModeGT, pagination.ModeGTE,
		pagination.ModeLT, pagination.ModeLTE, pagination.ModeBefore, pagination.ModeAfter,
	} {
		t.Run(string(mode), func(t *testing.T) {
			for _, sent := range timeOfDayProbes {
				t.Run(sent, func(t *testing.T) {
					t.Parallel()
					ref := parseClockProbe(sent)
					flt := pagination.Filter{Field: "at", Mode: mode, Value: sent, DataType: pagination.DataTypeTime}
					checkEvents(t, fx, flt, func(e Event) bool { return holds(mode, atClock(e), ref) })
				})
			}
		})
	}
	t.Run("range", func(t *testing.T) {
		for _, r := range [][2]string{{"00:00:00", "08:00:00"}, {"08:00", "09:15:30"}, {"12:30", "12:30"}, {"18:00", "23:59:59"}, {"23:59:59", "00:00:00"}, {"01:00", "06:59:59"}} {
			t.Run(r[0]+".."+r[1], func(t *testing.T) {
				t.Parallel()
				flt := pagination.Filter{Field: "at", Mode: pagination.ModeRange, DataType: pagination.DataTypeTime, Value: map[string]any{"from": r[0], "to": r[1]}}
				checkEvents(t, fx, flt, func(e Event) bool {
					return holds(pagination.ModeRange, atClock(e), parseClockProbe(r[0]), parseClockProbe(r[1]))
				})
			})
		}
	})
	t.Run("lists", func(t *testing.T) {
		for _, mode := range []pagination.Mode{pagination.ModeInside, pagination.ModeOutside} {
			for _, list := range [][]any{{"00:00:00"}, {"00:00:00", "23:59:59"}, {"12:30", "9:15 AM", "6:45 PM"}, {"01:02:03"}} {
				t.Run(fmt.Sprintf("%s/%v", mode, list), func(t *testing.T) {
					t.Parallel()
					refs := make([]time.Time, len(list))
					for i, s := range list {
						refs[i] = parseClockProbe(s.(string))
					}
					flt := pagination.Filter{Field: "at", Mode: mode, DataType: pagination.DataTypeTime, Value: list}
					checkEvents(t, fx, flt, func(e Event) bool { return holds(mode, atClock(e), refs...) })
				})
			}
		}
	})
}

// parseClockProbe is the oracle's reading of a time-of-day string.
func parseClockProbe(s string) time.Time {
	for _, layout := range []string{"15:04:05", "15:04", "3:04 PM"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	panic("bad clock probe " + s)
}
