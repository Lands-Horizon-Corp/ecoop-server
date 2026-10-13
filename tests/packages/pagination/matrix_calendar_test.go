//go:build integration

package pagination_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Cal is a calendar: one row at noon UTC for every day of 2024 (so a wrongly
// parsed date lands on a different row), plus rows on every awkward edge a
// calendar has: year, month and leap-day boundaries, a non-leap century, the
// epoch, 2038, the far future, and both daylight-saving changes in New York.
type Cal struct {
	bun.BaseModel `bun:"table:pg_cal"`

	ID        int64     `bun:"id,pk,autoincrement"`
	Title     string    `bun:"title,notnull"`
	At        time.Time `bun:"at,type:timestamptz,notnull"`
	Day       time.Time `bun:"day,type:date,notnull"`
	Clock     string    `bun:"clock,type:time,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

var cal pagination.IPaginationService[Cal, int64]

func calSeed() []Cal {
	seen := map[string]bool{}
	var out []Cal
	add := func(at time.Time) {
		title := at.Format(time.RFC3339Nano)
		if seen[title] {
			return
		}
		seen[title] = true
		out = append(out, Cal{
			Title: title, At: at, UpdatedAt: at,
			Day:   time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, time.UTC),
			Clock: at.Format("15:04:05.999999"),
		})
	}
	for d := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC); d.Year() == 2024; d = d.AddDate(0, 0, 1) {
		add(d)
	}
	for _, s := range []string{
		"2023-12-31T23:59:59Z", "2024-01-01T00:00:00Z", "2024-01-31T23:59:59Z", "2024-02-01T00:00:00Z",
		"2024-02-28T23:59:59Z", "2024-02-29T00:00:00Z", "2024-02-29T23:59:59.999999Z", "2024-03-01T00:00:00Z",
		"2024-03-31T23:59:59Z", "2024-04-01T00:00:00Z", "2024-04-30T23:59:59Z", "2024-05-01T00:00:00Z",
		"2024-06-30T23:59:59Z", "2024-07-01T00:00:00Z", "2024-12-31T23:59:59Z", "2025-01-01T00:00:00Z",
		"2023-02-28T23:59:59Z", "2023-03-01T00:00:00Z", "2100-02-28T12:00:00Z", "2100-03-01T00:00:00Z",
		"2000-02-29T12:00:00Z", "1999-12-31T23:59:59Z", "2000-01-01T00:00:00Z",
		"1970-01-01T00:00:00Z", "1969-12-31T23:59:59Z", "2038-01-19T03:14:07Z", "9999-12-31T23:59:59Z",
		// New York daylight saving: spring forward 2024-03-10 02:00 EST -> 03:00 EDT,
		// fall back 2024-11-03 02:00 EDT -> 01:00 EST. In UTC the clock never jumps.
		"2024-03-10T06:59:59Z", "2024-03-10T07:00:00Z", "2024-03-10T07:30:00Z",
		"2024-11-03T05:30:00Z", "2024-11-03T06:00:00Z", "2024-11-03T06:30:00Z",
	} {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			panic(err)
		}
		add(t)
	}
	return out
}

func setupCal() (func(), error) {
	s, cleanup, err := openDB((*Cal)(nil))
	if err != nil {
		return nil, err
	}
	rows := calSeed()
	ptrs := make([]*Cal, len(rows))
	for i := range rows {
		ptrs[i] = &rows[i]
	}
	if _, err := s.Client().NewInsert().Model(&ptrs).Exec(context.Background()); err != nil {
		cleanup()
		return nil, fmt.Errorf("seed cal: %w", err)
	}
	cal = pagination.NewPaginationService(&pagination.PaginationService[Cal, int64]{WriteSQLService: s}, utils.NewInflight())
	ledgerCleanup, err := setupLedger()
	if err != nil {
		cleanup()
		return nil, err
	}
	return func() { ledgerCleanup(); cleanup() }, nil
}

func needCal(t testing.TB) pagination.IPaginationService[Cal, int64] {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	if cal == nil {
		t.Skip("calendar fixture unavailable")
	}
	return cal
}

// checkRows runs sf through Paginate, Count, Exists and a cursor walk and
// compares each with the oracle over the seed.
func checkRows[T any](t *testing.T, svc pagination.IPaginationService[T, int64], seedRows []T, title func(T) string, sf pagination.StructuredFilter, match func(T) bool) {
	t.Helper()
	var want []string
	for _, r := range seedRows {
		if match(r) {
			want = append(want, title(r))
		}
	}
	titles := func(rows []*T) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = title(*r)
		}
		return out
	}
	ctx := ctxT(t)
	res, err := svc.Paginate(ctx, pagination.Pagination{Filter: sf, PageSize: 1000})
	if err != nil {
		t.Fatalf("Paginate: %v", err)
	}
	if !sameSet(titles(res.Data), want) {
		t.Fatalf("matched %d rows %v, want %d rows %v", len(res.Data), head(titles(res.Data)), len(want), head(want))
	}
	if n, err := svc.Count(ctx, sf); err != nil || int(n) != len(want) {
		t.Fatalf("Count = %d, %v; want %d", n, err, len(want))
	}
	if ok, err := svc.Exists(ctx, sf); err != nil || ok != (len(want) > 0) {
		t.Fatalf("Exists = %v, %v; want %v", ok, err, len(want) > 0)
	}
	walkSF := sf
	walkSF.SortFields = []pagination.SortField{asc("at")}
	var walked []string
	var cursor *string
	for range 100 {
		p, err := svc.Paginate(ctx, pagination.Pagination{Filter: walkSF, PageSize: 40, Cursor: cursor})
		if err != nil {
			t.Fatalf("walk: %v", err)
		}
		walked = append(walked, titles(p.Data)...)
		if cursor = p.NextCursor; cursor == nil {
			break
		}
	}
	if !sameSet(walked, want) || firstDuplicate(walked) != "" {
		t.Fatalf("cursor walk visited %d rows, want %d once each", len(walked), len(want))
	}
}

func head(s []string) []string {
	if len(s) > 6 {
		return append(append([]string{}, s[:6]...), fmt.Sprintf("...(%d more)", len(s)-6))
	}
	return s
}

func checkCal(t *testing.T, flt pagination.Filter, match func(Cal) bool) {
	t.Helper()
	checkCalFilter(t, and(flt), match)
}

func checkCalFilter(t *testing.T, sf pagination.StructuredFilter, match func(Cal) bool) {
	t.Helper()
	checkRows(t, needCal(t), calSeed(), func(c Cal) string { return c.Title }, sf, match)
}

func rejectedCal(t *testing.T, flt pagination.Filter) {
	t.Helper()
	svc := needCal(t)
	_, err1 := svc.Filter(ctxT(t), and(flt))
	_, err2 := svc.Paginate(ctxT(t), pagination.Pagination{Filter: and(flt)})
	for _, err := range []error{err1, err2} {
		if !errors.Is(err, pagination.ErrInvalidFilter) {
			t.Fatalf("err = %v, want ErrInvalidFilter", err)
		}
	}
}

// onDay matches rows whose instant falls on the given UTC day.
func onDay(d string) func(Cal) bool {
	ref := day(d)
	return func(c Cal) bool { return dayWide(pagination.ModeEqual, c.At, ref) }
}

func dateEq(field, sent string) pagination.Filter {
	return pagination.Filter{Field: field, Mode: pagination.ModeEqual, Value: sent, DataType: pagination.DataTypeDate}
}

// ---------------------------------------------------------------------------
// 1. Parsing & format ambiguities
// ---------------------------------------------------------------------------

// US vs international ordering. With slashes the month comes first, and the
// day only when that is the sole reading that exists. Run on a date column and
// on a timestamp column (where a bare date means the whole day).
func TestParse_FormatAmbiguity(t *testing.T) {
	cases := []struct{ sent, want string }{
		{"03/04/2024", "2024-03-04"}, // US: March 4th, not April 3rd
		{"04/03/2024", "2024-04-03"}, // US: April 3rd
		{"13/01/2024", "2024-01-13"}, // 13 cannot be a month, so day first
		{"01/13/2024", "2024-01-13"},
		{"12/31/2024", "2024-12-31"},
		{"31/12/2024", "2024-12-31"},
		{"02/29/2024", "2024-02-29"},
		{"29/02/2024", "2024-02-29"},
		{"12/12/2024", "2024-12-12"}, // both readings agree
		{"2024-03-04", "2024-03-04"}, // year first is always year-month-day
		{"2024/03/04", "2024-03-04"},
		{"2024-04-03", "2024-04-03"},
		{"03/04/24", ""},      // two-digit year: the century would be a guess
		{"00/10/2024", ""},    // no month 0
		{"March 4, 2024", ""}, // words are not understood
		{"4 Mar 2024", ""},
		{"2024-W10-1", ""}, // ISO week dates are not understood
	}
	for _, field := range []string{"day", "at"} {
		t.Run(field, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.sent, func(t *testing.T) {
					t.Parallel()
					flt := dateEq(field, tc.sent)
					if tc.want == "" {
						rejectedCal(t, flt)
						return
					}
					checkCal(t, flt, onDay(tc.want))
				})
			}
		})
	}
}

// Single digits, zero padding, and the separators - / . in the places they are
// used; spaces around the value are ignored; anything else is refused.
func TestParse_PaddingAndSeparators(t *testing.T) {
	cases := []struct{ sent, want string }{
		{"2024-3-1", "2024-03-01"},
		{"2024-03-1", "2024-03-01"},
		{"2024-3-01", "2024-03-01"},
		{"2024/3/1", "2024-03-01"},
		{"2024.03.01", "2024-03-01"},
		{"2024.3.1", "2024-03-01"},
		{"3/1/2024", "2024-03-01"},
		{"03/1/2024", "2024-03-01"},
		{"3/01/2024", "2024-03-01"},
		{"1.3.2024", "2024-03-01"}, // dots with the year last are day-first
		{"01.03.2024", "2024-03-01"},
		{"1-3-2024", "2024-03-01"}, // hyphens with the year last are day-first
		{"01-03-2024", "2024-03-01"},
		{"  2024-03-01  ", "2024-03-01"},
		{"\t2024-03-01\n", "2024-03-01"},
		{"2024_03_01", ""},
		{"2024 03 01", ""},
		{"2024--03-01", ""},
		{"2024-03-01-", ""},
		{"2024-003-01", ""},
		{"20240301", ""},
		{"2024-3", ""},
		{"2024", ""},
		{"3/1", ""},
		{"1.3.24", ""},
		{"２０２４-03-01", ""}, // full-width digits
		{"2024-03-01 ", "2024-03-01"},
	}
	for _, field := range []string{"day", "at"} {
		t.Run(field, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("%q", tc.sent), func(t *testing.T) {
					t.Parallel()
					flt := dateEq(field, tc.sent)
					if tc.want == "" {
						rejectedCal(t, flt)
						return
					}
					checkCal(t, flt, onDay(tc.want))
				})
			}
		})
	}
}

// Times: one or two hour digits, 12- and 24-hour clocks, any case of am/pm.
func TestParse_TimePadding(t *testing.T) {
	svc := needLogs(t)
	cases := []struct {
		sent string
		want string // "" = refused; otherwise the time of day it means
	}{
		{"6:00", "06:00"}, {"06:00", "06:00"}, {"6:00:00", "06:00"}, {"06:00:00", "06:00"},
		{"6:00 AM", "06:00"}, {"06:00 AM", "06:00"}, {"6:00 am", "06:00"}, {"6:00 Am", "06:00"},
		{"6:00AM", "06:00"}, {"6:00 PM", "18:00"}, {"06:00 PM", "18:00"}, {"6:00 pm", "18:00"},
		{"12:00 AM", "00:00"}, {"12:00 PM", "12:00"}, {"12:00 am", "00:00"}, {"12:00 pm", "12:00"},
		{"  6:00  ", "06:00"},
		{"6:5", ""}, {"6:00:5", ""}, {"24:00", ""}, {"12:60", ""}, {"25:00:00", ""},
		{"13:00 PM", ""}, {"6:00 XM", ""}, {"6.00", ""}, {"600", ""}, {"6", ""}, {"six", ""},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%q", tc.sent), func(t *testing.T) {
			t.Parallel()
			flt := pagination.Filter{Field: "at", Mode: pagination.ModeEqual, Value: tc.sent, DataType: pagination.DataTypeTime}
			if tc.want == "" {
				_, err := svc.Filter(ctxT(t), and(flt))
				if !errors.Is(err, pagination.ErrInvalidFilter) {
					t.Fatalf("err = %v, want ErrInvalidFilter", err)
				}
				return
			}
			ref := parseTOD(tc.want)
			checkLogs(t, svc, and(flt), func(l Log) bool { return tod(l.At).Equal(ref) })
		})
	}
}

// ---------------------------------------------------------------------------
// 2. Time zones & offsets
// ---------------------------------------------------------------------------

// Every way to write the same instant, 2024-03-10T12:00:00Z, finds the same
// row: numeric offsets from -12:00 to +14:00 (including the half and quarter
// hour ones), with and without a colon, in several layouts.
func TestTZ_SameInstantManyOffsets(t *testing.T) {
	const target = "2024-03-10T12:00:00Z"
	want := instant(target)
	sent := []string{
		"2024-03-10T12:00:00Z",
		"2024-03-10T12:00:00+00:00",
		"2024-03-10T12:00:00-00:00",
		"2024-03-10T07:00:00-05:00",
		"2024-03-10T00:00:00-12:00",
		"2024-03-11T02:00:00+14:00",
		"2024-03-11T01:00:00+13:00",
		"2024-03-10T17:30:00+05:30",
		"2024-03-10T17:45:00+05:45",
		"2024-03-10T08:30:00-03:30",
		"2024-03-10T02:30:00-09:30",
		"2024-03-10T14:00:00+02:00",
		"Sun, 10 Mar 2024 17:30:00 +0530",
		"Sun, 10 Mar 2024 12:00:00 +0000",
		"Sun, 10 Mar 2024 12:00:00 UTC",
		"Sun, 10 Mar 2024 12:00:00 GMT",
		"10 Mar 24 07:00 -0500",
		"2024-03-10T12:00:00.000000Z",
		"2024-03-10 12:00:00",
		"2024-03-10T12:00:00",
	}
	for _, s := range sent {
		t.Run(s, func(t *testing.T) {
			t.Parallel()
			checkCal(t, dateEq("at", s), func(c Cal) bool { return c.At.Equal(want) })
			// And the instant really is one row: it sits strictly between its neighbours.
			gt := pagination.Filter{Field: "at", Mode: pagination.ModeGT, Value: s, DataType: pagination.DataTypeDate}
			checkCal(t, gt, func(c Cal) bool { return c.At.After(want) })
		})
	}
}

// A zone written as an abbreviation depends on the server's own zone (or on
// nothing at all, for unknown ones), so it is refused; Z, UTC, GMT and numeric
// offsets are the unambiguous spellings.
func TestTZ_AbbreviationsAreRefused(t *testing.T) {
	for _, zone := range []string{"EST", "EDT", "PST", "PDT", "CST", "CET", "IST", "BST", "JST", "AEST", "XYZ"} {
		t.Run(zone, func(t *testing.T) {
			t.Parallel()
			rejectedCal(t, dateEq("at", "Sun, 10 Mar 2024 12:00:00 "+zone))
		})
	}
	for _, zone := range []string{"UTC", "GMT"} {
		t.Run(zone+" is fine", func(t *testing.T) {
			t.Parallel()
			checkCal(t, dateEq("at", "Sun, 10 Mar 2024 12:00:00 "+zone), func(c Cal) bool { return c.At.Equal(instant("2024-03-10T12:00:00Z")) })
		})
	}
	t.Run("a time of day with an abbreviation is refused too", func(t *testing.T) {
		t.Parallel()
		svc := needLogs(t)
		_, err := svc.Filter(ctxT(t), and(pagination.Filter{Field: "at", Mode: pagination.ModeEqual, Value: "12:00:00 EST", DataType: pagination.DataTypeTime}))
		if !errors.Is(err, pagination.ErrInvalidFilter) {
			t.Fatalf("err = %v, want ErrInvalidFilter", err)
		}
	})
}

// A value with no zone is UTC, not the server's local time: the answer must
// not depend on where the process runs.
func TestTZ_NaiveValuesAreUTC(t *testing.T) {
	want := instant("2024-03-10T12:00:00Z")
	for _, s := range []string{
		"2024-03-10 12:00:00", "2024-03-10T12:00:00", "2024/03/10 12:00:00", "03/10/2024 12:00:00",
		"2024-03-10T12:00:00.0", "2024-03-10T12:00:00.000000000",
	} {
		t.Run(s, func(t *testing.T) {
			t.Parallel()
			checkCal(t, dateEq("at", s), func(c Cal) bool { return c.At.Equal(want) })
		})
	}
	t.Run("a bare date is a UTC day whatever the local zone", func(t *testing.T) {
		old := time.Local
		defer func() { time.Local = old }()
		for _, zone := range []string{"Pacific/Kiritimati", "Pacific/Pago_Pago", "Asia/Kolkata", "America/St_Johns"} {
			loc, err := time.LoadLocation(zone)
			if err != nil {
				t.Skip("tzdata unavailable")
			}
			time.Local = loc
			checkCal(t, dateEq("at", "2024-03-10"), onDay("2024-03-10"))
		}
	})
}

// An offset moves an instant across midnight; the day it lands on is the UTC
// day, not the day written.
func TestTZ_OffsetCrossesMidnight(t *testing.T) {
	cases := []struct {
		name, sent string
		mode       pagination.Mode
		utc        string
	}{
		{"+01:00 just after local midnight is the previous UTC day", "2024-01-01T00:59:59+01:00", pagination.ModeLTE, "2023-12-31T23:59:59Z"},
		{"lt the same instant excludes it", "2024-01-01T00:59:59+01:00", pagination.ModeLT, "2023-12-31T23:59:59Z"},
		{"-05:00 evening is the next UTC day", "2023-12-31T19:00:00-05:00", pagination.ModeEqual, "2024-01-01T00:00:00Z"},
		{"+14:00 morning is the previous UTC day", "2024-03-01T13:59:59+14:00", pagination.ModeEqual, "2024-02-29T23:59:59Z"},
		{"-12:00 late evening", "2024-02-29T12:00:00-12:00", pagination.ModeEqual, "2024-03-01T00:00:00Z"},
		{"gte across the leap boundary", "2024-02-29T18:59:59-05:00", pagination.ModeGTE, "2024-02-29T23:59:59Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ref := instant(tc.utc)
			flt := pagination.Filter{Field: "at", Mode: tc.mode, Value: tc.sent, DataType: pagination.DataTypeDate}
			checkCal(t, flt, func(c Cal) bool { return holds(tc.mode, c.At, ref) })
		})
	}
}

// ---------------------------------------------------------------------------
// 3. Daylight-saving transitions (New York, 2024)
// ---------------------------------------------------------------------------

// Spring forward: local 02:00-02:59 on 2024-03-10 never happens. In UTC the
// timeline has no gap: 06:59:59Z (01:59:59 EST) is followed one second later by
// 07:00:00Z (03:00:00 EDT).
func TestDST_SpringForward(t *testing.T) {
	at := func(mode pagination.Mode, v string) pagination.Filter {
		return pagination.Filter{Field: "at", Mode: mode, Value: v, DataType: pagination.DataTypeDate}
	}
	type tc struct {
		name string
		flt  pagination.Filter
		want func(Cal) bool
	}
	eq := func(utc string) func(Cal) bool { return func(c Cal) bool { return c.At.Equal(instant(utc)) } }
	rng := func(lo, hi string) func(Cal) bool {
		return func(c Cal) bool { return !c.At.Before(instant(lo)) && !c.At.After(instant(hi)) }
	}
	cases := []tc{
		{"last second before: 01:59:59 EST", at(pagination.ModeEqual, "2024-03-10T01:59:59-05:00"), eq("2024-03-10T06:59:59Z")},
		{"first second after: 03:00:00 EDT", at(pagination.ModeEqual, "2024-03-10T03:00:00-04:00"), eq("2024-03-10T07:00:00Z")},
		{"the two are one second apart", pagination.Filter{Field: "at", Mode: pagination.ModeRange, DataType: pagination.DataTypeDate,
			Value: map[string]any{"from": "2024-03-10T01:59:59-05:00", "to": "2024-03-10T03:00:00-04:00"}}, rng("2024-03-10T06:59:59Z", "2024-03-10T07:00:00Z")},
		{"a local time inside the gap, as EST", at(pagination.ModeEqual, "2024-03-10T02:30:00-05:00"), eq("2024-03-10T07:30:00Z")},
		{"a local time inside the gap, as EDT", at(pagination.ModeEqual, "2024-03-10T02:30:00-04:00"), eq("2024-03-10T06:30:00Z")},
		{"a naive time inside the gap is simply UTC", at(pagination.ModeEqual, "2024-03-10T02:30:00"), eq("2024-03-10T02:30:00Z")},
		{"after the last EST second", at(pagination.ModeGT, "2024-03-10T01:59:59-05:00"), func(c Cal) bool { return c.At.After(instant("2024-03-10T06:59:59Z")) }},
		{"before the first EDT second", at(pagination.ModeLT, "2024-03-10T03:00:00-04:00"), func(c Cal) bool { return c.At.Before(instant("2024-03-10T07:00:00Z")) }},
		{"the day is 24 hours long", at(pagination.ModeEqual, "2024-03-10"), onDay("2024-03-10")},
		{"the window across the gap, local to local", pagination.Filter{Field: "at", Mode: pagination.ModeRange, DataType: pagination.DataTypeDate,
			Value: map[string]any{"from": "2024-03-10T00:00:00-05:00", "to": "2024-03-10T23:59:59-04:00"}}, rng("2024-03-10T05:00:00Z", "2024-03-11T03:59:59Z")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { t.Parallel(); checkCal(t, c.flt, c.want) })
	}
}

// Fall back: local 01:00-01:59 on 2024-11-03 happens twice. The same wall
// clock is two different instants, told apart only by the offset.
func TestDST_FallBack(t *testing.T) {
	at := func(mode pagination.Mode, v string) pagination.Filter {
		return pagination.Filter{Field: "at", Mode: mode, Value: v, DataType: pagination.DataTypeDate}
	}
	eq := func(utc string) func(Cal) bool { return func(c Cal) bool { return c.At.Equal(instant(utc)) } }
	cases := []struct {
		name string
		flt  pagination.Filter
		want func(Cal) bool
	}{
		{"01:30 EDT is the first pass", at(pagination.ModeEqual, "2024-11-03T01:30:00-04:00"), eq("2024-11-03T05:30:00Z")},
		{"01:30 EST is the second pass", at(pagination.ModeEqual, "2024-11-03T01:30:00-05:00"), eq("2024-11-03T06:30:00Z")},
		{"01:30 EDT is not 01:30 EST", at(pagination.ModeNotEqual, "2024-11-03T01:30:00-04:00"), func(c Cal) bool { return !c.At.Equal(instant("2024-11-03T05:30:00Z")) }},
		{"between the two passes", pagination.Filter{Field: "at", Mode: pagination.ModeRange, DataType: pagination.DataTypeDate,
			Value: map[string]any{"from": "2024-11-03T01:30:00-04:00", "to": "2024-11-03T01:30:00-05:00"}},
			func(c Cal) bool {
				return !c.At.Before(instant("2024-11-03T05:30:00Z")) && !c.At.After(instant("2024-11-03T06:30:00Z"))
			}},
		{"a reversed pair is empty", pagination.Filter{Field: "at", Mode: pagination.ModeRange, DataType: pagination.DataTypeDate,
			Value: map[string]any{"from": "2024-11-03T01:30:00-05:00", "to": "2024-11-03T01:30:00-04:00"}}, func(Cal) bool { return false }},
		{"01:00 EST is 06:00Z", at(pagination.ModeEqual, "2024-11-03T01:00:00-05:00"), eq("2024-11-03T06:00:00Z")},
		{"after the first 01:30", at(pagination.ModeGT, "2024-11-03T01:30:00-04:00"), func(c Cal) bool { return c.At.After(instant("2024-11-03T05:30:00Z")) }},
		{"the repeated hour both fall on one UTC day", at(pagination.ModeEqual, "2024-11-03"), onDay("2024-11-03")},
		{"a list of both passes", pagination.Filter{Field: "at", Mode: pagination.ModeInside, DataType: pagination.DataTypeDate,
			Value: []any{"2024-11-03T01:30:00-04:00", "2024-11-03T01:30:00-05:00"}},
			func(c Cal) bool {
				return c.At.Equal(instant("2024-11-03T05:30:00Z")) || c.At.Equal(instant("2024-11-03T06:30:00Z"))
			}},
		{"the same wall time with no offset is one UTC instant", at(pagination.ModeEqual, "2024-11-03T01:30:00"), eq("2024-11-03T01:30:00Z")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { t.Parallel(); checkCal(t, c.flt, c.want) })
	}
}

// ---------------------------------------------------------------------------
// 4. Impossible dates
// ---------------------------------------------------------------------------

func TestCalendar_ImpossibleDatesAreRefused(t *testing.T) {
	bad := []string{
		"2023-02-29", "2100-02-29", "1900-02-29", "2024-02-30", "2024-02-31", "2024-04-31",
		"2024-06-31", "2024-09-31", "2024-11-31", "2024-00-10", "2024-13-01", "2024-01-00",
		"2024-01-32", "02/30/2024", "04/31/2024", "30/02/2024", "31/04/2024", "2023/02/29",
		"29.02.2023", "29-02-2023", "2024-02-29T24:00:00Z", "2024-02-30T12:00:00Z",
	}
	modes := []pagination.Mode{pagination.ModeEqual, pagination.ModeGT, pagination.ModeLTE, pagination.ModeBefore}
	for _, s := range bad {
		for _, mode := range modes {
			t.Run(fmt.Sprintf("%s/%s", mode, s), func(t *testing.T) {
				t.Parallel()
				rejectedCal(t, pagination.Filter{Field: "day", Mode: mode, Value: s, DataType: pagination.DataTypeDate})
			})
		}
	}
	t.Run("in a range bound", func(t *testing.T) {
		t.Parallel()
		rejectedCal(t, pagination.Filter{Field: "day", Mode: pagination.ModeRange, DataType: pagination.DataTypeDate, Value: map[string]any{"from": "2024-01-01", "to": "2023-02-29"}})
	})
	t.Run("in a list", func(t *testing.T) {
		t.Parallel()
		rejectedCal(t, pagination.Filter{Field: "day", Mode: pagination.ModeInside, DataType: pagination.DataTypeDate, Value: []any{"2024-02-29", "2024-04-31"}})
	})
}

// The leap days that do exist are accepted.
func TestCalendar_RealLeapDays(t *testing.T) {
	for _, d := range []string{"2024-02-29", "2000-02-29", "2020-02-29", "2096-02-29"} {
		for _, form := range []string{d, strings.ReplaceAll(d, "-", "/"), d + "T00:00:00Z"} {
			t.Run(form, func(t *testing.T) {
				t.Parallel()
				checkCal(t, dateEq("at", form), func(c Cal) bool {
					if strings.Contains(form, "T") {
						return c.At.Equal(instant(form))
					}
					return dayWide(pagination.ModeEqual, c.At, day(d))
				})
			})
		}
	}
	t.Run("the day after a leap day exists in a leap year and not in the century", func(t *testing.T) {
		t.Parallel()
		checkCal(t, dateEq("at", "2100-03-01"), onDay("2100-03-01"))
		rejectedCal(t, dateEq("at", "2100-02-29"))
	})
}

// ---------------------------------------------------------------------------
// 5. Rollovers & boundaries
// ---------------------------------------------------------------------------

// Year, month, leap-day, century, epoch, 2038 and far-future boundaries, each
// with the comparisons that sit exactly on them.
func TestCalendar_Rollovers(t *testing.T) {
	probes := []string{
		"2023-12-31", "2024-01-01", "2024-01-31", "2024-02-01", "2024-02-28", "2024-02-29",
		"2024-03-01", "2024-03-31", "2024-04-01", "2024-04-30", "2024-05-01", "2024-06-30",
		"2024-07-01", "2024-12-31", "2025-01-01", "2023-02-28", "2023-03-01", "2100-02-28",
		"2100-03-01", "2000-02-29", "1999-12-31", "2000-01-01", "1970-01-01", "1969-12-31",
		"2038-01-19", "9999-12-31",
	}
	modes := []pagination.Mode{pagination.ModeEqual, pagination.ModeNotEqual, pagination.ModeGT, pagination.ModeGTE, pagination.ModeLT, pagination.ModeLTE}
	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			for _, p := range probes {
				t.Run(p, func(t *testing.T) {
					t.Parallel()
					ref := day(p)
					checkCal(t, pagination.Filter{Field: "at", Mode: mode, Value: p, DataType: pagination.DataTypeDate},
						func(c Cal) bool { return dayWide(mode, c.At, ref) })
				})
			}
		})
	}
}

// Ranges that straddle a boundary include both sides of it.
func TestCalendar_RangesAcrossBoundaries(t *testing.T) {
	cases := []struct{ name, from, to string }{
		{"new year", "2023-12-31", "2024-01-01"},
		{"january into february", "2024-01-31", "2024-02-01"},
		{"leap day and its neighbours", "2024-02-28", "2024-03-01"},
		{"non-leap february into march", "2023-02-28", "2023-03-01"},
		{"the century that is not a leap year", "2100-02-28", "2100-03-01"},
		{"the millennium", "1999-12-31", "2000-01-01"},
		{"the epoch", "1969-12-31", "1970-01-01"},
		{"a 30-day month into a 31-day month", "2024-04-30", "2024-05-01"},
		{"half year", "2024-06-30", "2024-07-01"},
		{"the whole of 2024", "2024-01-01", "2024-12-31"},
		{"a single bare day is the whole day", "2024-02-29", "2024-02-29"},
		{"the far future", "9999-12-30", "9999-12-31"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lo, hi := day(tc.from), day(tc.to).AddDate(0, 0, 1)
			flt := pagination.Filter{Field: "at", Mode: pagination.ModeRange, DataType: pagination.DataTypeDate, Value: map[string]any{"from": tc.from, "to": tc.to}}
			checkCal(t, flt, func(c Cal) bool { return !c.At.Before(lo) && c.At.Before(hi) })
		})
	}
}
