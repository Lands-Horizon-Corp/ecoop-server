//go:build integration

package pagination_test

import (
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
)

func tF(mode pagination.Mode, v any) pagination.Filter {
	return pagination.Filter{Field: "at", Mode: mode, Value: v, DataType: pagination.DataTypeTime}
}

func dF(mode pagination.Mode, v any) pagination.Filter {
	return pagination.Filter{Field: "at", Mode: mode, Value: v, DataType: pagination.DataTypeDate}
}

// inTOD is the rule "the row's time of day lies in [from, to]".
func inTOD(from, to string) func(Log) bool {
	lo, hi := parseTOD(from), parseTOD(to)
	return func(l Log) bool { return holds(pagination.ModeRange, tod(l.At), lo, hi) }
}

// ---------------------------------------------------------------------------
// 6. Range & filter boundaries (time of day)
// ---------------------------------------------------------------------------

// Ranges over a time of day include both ends, to the microsecond.
func TestRangeTime_Boundaries(t *testing.T) {
	svc := needLogs(t)
	cases := []struct {
		name     string
		from, to string
	}{
		{"a single instant: midnight", "00:00", "00:00"},
		{"a single second that no row has", "23:59:59", "23:59:59"},
		{"the last second including its microseconds", "23:59:59", "23:59:59.999999"},
		{"noon through one microsecond past it", "12:00", "12:00:00.000001"},
		{"exactly one microsecond", "12:00:00.000001", "12:00:00.000001"},
		{"a reversed pair is empty (no wrap past midnight)", "18:00", "06:00"},
		{"the whole day", "00:00", "23:59:59.999999"},
		{"morning in 12-hour clock", "12:00 AM", "11:59 AM"},
		{"both ends inclusive", "06:00", "18:00"},
		{"strictly after midnight, before 6am", "00:00:00.000001", "05:59:59.999999"},
		{"the half second only", "00:00:00.4", "00:00:00.6"},
		{"afternoon in 12-hour clock", "12:00 PM", "6:00 PM"},
		{"hours and seconds mixed formats", "6:00", "18:00:00"},
		{"nothing between 06:00:00.000001 and 11:59:59", "06:00:00.000001", "11:59:59"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			flt := tF(pagination.ModeRange, map[string]any{"from": tc.from, "to": tc.to})
			checkLogs(t, svc, and(flt), inTOD(tc.from, tc.to))
		})
	}
	t.Run("two adjacent ranges partition the day", func(t *testing.T) {
		t.Parallel()
		ctx := ctxT(t)
		count := func(from, to string) int64 {
			n, err := svc.Count(ctx, and(tF(pagination.ModeRange, map[string]any{"from": from, "to": to})))
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
		morning, afternoon := count("00:00", "11:59:59.999999"), count("12:00", "23:59:59.999999")
		all, err := svc.Count(ctx, and())
		if err != nil {
			t.Fatal(err)
		}
		if morning+afternoon != all {
			t.Fatalf("%d + %d != %d: a row fell in the gap or in both", morning, afternoon, all)
		}
		overlap, err := svc.Count(ctx, and(
			tF(pagination.ModeRange, map[string]any{"from": "00:00", "to": "11:59:59.999999"}),
			tF(pagination.ModeRange, map[string]any{"from": "12:00", "to": "23:59:59.999999"}),
		))
		if err != nil || overlap != 0 {
			t.Fatalf("the halves overlap by %d rows, %v", overlap, err)
		}
	})
	t.Run("range and its complement cover every row exactly once", func(t *testing.T) {
		t.Parallel()
		ctx := ctxT(t)
		in, _ := svc.Count(ctx, and(tF(pagination.ModeRange, map[string]any{"from": "06:00", "to": "18:00"})))
		out, _ := svc.Count(ctx, or(tF(pagination.ModeLT, "06:00"), tF(pagination.ModeGT, "18:00")))
		all, _ := svc.Count(ctx, and())
		if in+out != all {
			t.Fatalf("%d inside + %d outside != %d", in, out, all)
		}
	})
}

// inside / outside lists on a time of day compare to the microsecond too.
func TestRangeTime_Lists(t *testing.T) {
	svc := needLogs(t)
	cases := []struct {
		name string
		mode pagination.Mode
		list []any
	}{
		{"inside the microsecond row", pagination.ModeInside, []any{"12:00:00.000001"}},
		{"inside whole noon, not the microsecond row", pagination.ModeInside, []any{"12:00:00"}},
		{"inside both", pagination.ModeInside, []any{"12:00:00", "12:00:00.000001"}},
		{"inside the half second", pagination.ModeInside, []any{"00:00:00.5"}},
		{"inside 12-hour spellings of one time", pagination.ModeInside, []any{"6:00 PM", "18:00", "6:00:00 PM"}},
		{"outside the six-hourly marks", pagination.ModeOutside, []any{"00:00", "06:00", "12:00", "18:00"}},
		{"outside the last microsecond", pagination.ModeOutside, []any{"23:59:59.999999"}},
		{"outside nothing seeded", pagination.ModeOutside, []any{"01:02:03"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			refs := make([]time.Time, len(tc.list))
			for i, s := range tc.list {
				refs[i] = parseTOD(s.(string))
			}
			checkLogs(t, svc, and(tF(tc.mode, tc.list)), func(l Log) bool { return holds(tc.mode, tod(l.At), refs...) })
		})
	}
}

// ---------------------------------------------------------------------------
// 7. Range truncation: a time-of-day edge meeting a full-timestamp edge
// ---------------------------------------------------------------------------

func TestRangeTruncation(t *testing.T) {
	svc := needLogs(t)
	jan5 := func(h, m, s, ns int) time.Time { return time.Date(2024, 1, 5, h, m, s, ns, time.UTC) }
	cases := []struct {
		name  string
		sf    pagination.StructuredFilter
		match func(Log) bool
	}{
		{"a bare end day keeps the whole of its last day",
			and(dF(pagination.ModeRange, map[string]any{"from": "2024-01-04", "to": "2024-01-05"})),
			func(l Log) bool {
				return !l.At.Before(jan5(0, 0, 0, 0).AddDate(0, 0, -1)) && l.At.Before(jan5(0, 0, 0, 0).AddDate(0, 0, 1))
			}},
		{"an end instant at midnight cuts the day off at once",
			and(dF(pagination.ModeRange, map[string]any{"from": "2024-01-04", "to": "2024-01-05T00:00:00Z"})),
			func(l Log) bool {
				return !l.At.Before(jan5(0, 0, 0, 0).AddDate(0, 0, -1)) && !l.At.After(jan5(0, 0, 0, 0))
			}},
		{"a timed start with a bare end",
			and(dF(pagination.ModeRange, map[string]any{"from": "2024-01-05T12:00:00Z", "to": "2024-01-05"})),
			func(l Log) bool {
				return !l.At.Before(jan5(12, 0, 0, 0)) && l.At.Before(jan5(0, 0, 0, 0).AddDate(0, 0, 1))
			}},
		{"an end one microsecond after noon",
			and(dF(pagination.ModeRange, map[string]any{"from": "2024-01-05", "to": "2024-01-05T12:00:00.000001Z"})),
			func(l Log) bool { return !l.At.Before(jan5(0, 0, 0, 0)) && !l.At.After(jan5(12, 0, 0, 1_000)) }},
		{"a day and the late afternoon of every day",
			and(dF(pagination.ModeRange, map[string]any{"from": "2024-01-05", "to": "2024-01-05"}), tF(pagination.ModeGTE, "12:00")),
			func(l Log) bool { return l.At.Day() == 5 && tod(l.At).Compare(parseTOD("12:00")) >= 0 }},
		{"an end instant at midnight leaves no afternoon",
			and(dF(pagination.ModeRange, map[string]any{"from": "2024-01-05", "to": "2024-01-05T00:00:00Z"}), tF(pagination.ModeGTE, "12:00")),
			func(Log) bool { return false }},
		{"the day's first half second",
			and(dF(pagination.ModeEqual, "2024-01-05"), tF(pagination.ModeLT, "00:00:00.5")),
			func(l Log) bool { return l.At.Equal(jan5(0, 0, 0, 0)) }},
		{"the day's last microsecond by time range",
			and(dF(pagination.ModeEqual, "2024-01-05"), tF(pagination.ModeRange, map[string]any{"from": "23:59:59", "to": "23:59:59.999999"})),
			func(l Log) bool { return l.At.Equal(jan5(23, 59, 59, 999_999_000)) }},
		{"every midnight after the 5th",
			and(dF(pagination.ModeGT, "2024-01-05"), tF(pagination.ModeEqual, "00:00")),
			func(l Log) bool { return l.At.Day() > 5 && tod(l.At).Equal(parseTOD("00:00")) }},
		{"nothing is later than the last microsecond of the 5th and also on or before the 5th",
			and(dF(pagination.ModeLTE, "2024-01-05"), tF(pagination.ModeGT, "23:59:59.999999")),
			func(Log) bool { return false }},
		{"a time of day alone ignores the date",
			and(tF(pagination.ModeEqual, "12:00")),
			func(l Log) bool { return tod(l.At).Equal(parseTOD("12:00")) }},
		{"a seventh fractional digit is truncated, not rounded: equal 00:00:00.0000009 is midnight",
			and(tF(pagination.ModeEqual, "00:00:00.0000009")),
			func(l Log) bool { return tod(l.At).Equal(parseTOD("00:00")) }},
		{"a seventh fractional digit is truncated: 12:00:00.0000019 is the microsecond row",
			and(tF(pagination.ModeEqual, "12:00:00.0000019")),
			func(l Log) bool { return tod(l.At).Equal(parseTOD("12:00:00.000001")) }},
		{"a date equal OR a time equal widens, it does not intersect",
			or(dF(pagination.ModeEqual, "2024-01-05"), tF(pagination.ModeEqual, "00:00")),
			func(l Log) bool { return l.At.Day() == 5 || tod(l.At).Equal(parseTOD("00:00")) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			checkLogs(t, svc, tc.sf, tc.match)
		})
	}
}

// A time of day written with an offset is read in UTC, so it lines up with a
// timestamptz column compared in UTC: "07:00:00+01:00" is 06:00 UTC.
func TestTZ_TimeOfDayOffsets(t *testing.T) {
	svc := needLogs(t)
	var tz string
	if err := logsDB().NewRaw("SHOW timezone").Scan(ctxT(t), &tz); err != nil {
		t.Fatal(err)
	}
	if tz != "UTC" && tz != "Etc/UTC" {
		t.Skipf("session time zone is %s; timestamptz::time follows it", tz)
	}
	cases := []struct{ sent, utc string }{
		{"07:00:00+01:00", "06:00"},
		{"13:00:00-05:00", "18:00"},
		{"02:00:00+02:00", "00:00"},
		{"19:00:00+01:00", "18:00"},
		{"01:00:00-05:00", "06:00"},
		{"12:00:00Z", "12:00"},
		{"12:00:00+00:00", "12:00"},
		{"00:30:00+00:30", "00:00"},
		{"05:45:00+05:45", "00:00"},
		{"12:00:00 UTC", "12:00"},
		{"12:00:00 GMT", "12:00"},
		{"01:00:00+02:00", "23:00"}, // wraps to the previous day's evening
		{"23:00:00-05:00", "04:00"}, // wraps to the next day's morning
	}
	for _, tc := range cases {
		t.Run(tc.sent, func(t *testing.T) {
			t.Parallel()
			ref := parseTOD(tc.utc)
			checkLogs(t, svc, and(tF(pagination.ModeEqual, tc.sent)), func(l Log) bool { return tod(l.At).Equal(ref) })
		})
	}
	t.Run("a range with offset bounds", func(t *testing.T) {
		t.Parallel()
		flt := tF(pagination.ModeRange, map[string]any{"from": "07:00:00+01:00", "to": "13:00:00+01:00"})
		checkLogs(t, svc, and(flt), inTOD("06:00", "12:00"))
	})
	t.Run("a list with mixed offsets", func(t *testing.T) {
		t.Parallel()
		ref := []time.Time{parseTOD("06:00"), parseTOD("18:00")}
		flt := tF(pagination.ModeInside, []any{"07:00:00+01:00", "13:00:00-05:00"})
		checkLogs(t, svc, and(flt), func(l Log) bool { return holds(pagination.ModeInside, tod(l.At), ref...) })
	})
}
