//go:build integration

package pagination_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Event carries one value of each temporal column type, so a date filter is
// checked against a date, a timestamptz and a time of day.
type Event struct {
	bun.BaseModel `bun:"table:pg_events"`

	ID        int64     `bun:"id,pk,autoincrement"`
	Title     string    `bun:"title,notnull"`
	Day       time.Time `bun:"day,type:date,notnull"`
	At        time.Time `bun:"at,type:timestamptz,notnull"`
	Clock     string    `bun:"clock,type:time,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

type eventFixture struct {
	svc pagination.IPaginationService[Event, int64]
	db  *bun.DB
}

func (e *eventFixture) sqlDB() *bun.DB { return e.db }

var (
	events     *eventFixture
	eventsSkip string
)

// eventSeed: boundaries that break date logic: year end and start, a leap
// day, a DST change, month ends, midnight and the last second of a day.
func eventSeed() []Event {
	row := func(title, at, clock string) Event {
		t, err := time.Parse(time.RFC3339, at)
		if err != nil {
			panic(err)
		}
		return Event{
			Title: title, At: t, Clock: clock, UpdatedAt: t,
			Day: time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC),
		}
	}
	return []Event{
		row("new-year-eve", "2023-12-31T23:59:59Z", "23:59:59"),
		row("new-year", "2024-01-01T00:00:00Z", "00:00:00"),
		row("mid-jan", "2024-01-15T12:30:00Z", "12:30:00"),
		row("end-jan", "2024-01-31T23:59:59Z", "23:59:59"),
		row("leap-day", "2024-02-29T08:00:00Z", "08:00:00"),
		row("march-1", "2024-03-01T00:00:00Z", "00:00:00"),
		row("us-dst", "2024-03-10T07:00:00Z", "07:00:00"),
		row("june-30", "2024-06-30T18:45:00Z", "18:45:00"),
		row("july-4", "2024-07-04T09:15:30Z", "09:15:30"),
		row("oct-31", "2024-10-31T21:00:00Z", "21:00:00"),
		row("dec-31", "2024-12-31T23:59:59Z", "23:59:59"),
		row("next-year", "2025-01-01T00:00:00Z", "00:00:00"),
	}
}

func setupEvents() (func(), error) {
	if skipReason != "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, cleanup, err := openDB((*Event)(nil))
	if err != nil {
		return nil, err
	}
	rows := eventSeed()
	ptrs := make([]*Event, len(rows))
	for i := range rows {
		ptrs[i] = &rows[i]
	}
	if _, err := s.Client().NewInsert().Model(&ptrs).Exec(ctx); err != nil {
		cleanup()
		return nil, fmt.Errorf("seed events: %w", err)
	}
	events = &eventFixture{db: s.Client(), svc: pagination.NewPaginationService(&pagination.PaginationService[Event, int64]{WriteSQLService: s}, utils.NewInflight())}
	logCleanup, err := setupLogs()
	if err != nil {
		cleanup()
		return nil, err
	}
	return func() { logCleanup(); cleanup() }, nil
}

func needEvents(t testing.TB) *eventFixture {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	if events == nil {
		t.Skip(eventsSkip)
	}
	return events
}

func day(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func instant(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func clock(s string) time.Time {
	for _, layout := range []string{"15:04:05", "15:04"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	panic("bad clock " + s)
}

// column describes how to filter one temporal column and how to read the
// same value back from a seeded row.
type column struct {
	field string
	typ   pagination.DataType
	key   func(Event) time.Time
}

var (
	colDay   = column{"day", pagination.DataTypeDate, func(e Event) time.Time { return e.Day }}
	colAt    = column{"at", pagination.DataTypeDate, func(e Event) time.Time { return e.At }}
	colClock = column{"clock", pagination.DataTypeTime, func(e Event) time.Time { return clock(e.Clock) }}
)

// probe is a value as a client sends it and what that value means.
type probe struct {
	col  column
	sent string
	ref  time.Time
}

// probes are the values every comparison mode is exercised with: ISO dates,
// slash layouts, RFC3339 with and without offsets, naive timestamps, 12-hour
// clocks, and values before, between and after every seeded row.
var probes = []probe{
	{colDay, "2023-12-31", day("2023-12-31")},
	{colDay, "2024-01-01", day("2024-01-01")},
	{colDay, "2024-01-15", day("2024-01-15")},
	{colDay, "2024-02-29", day("2024-02-29")},
	{colDay, "2024-03-01", day("2024-03-01")},
	{colDay, "2024-06-30", day("2024-06-30")},
	{colDay, "2025-01-01", day("2025-01-01")},
	{colDay, "1999-01-01", day("1999-01-01")},
	{colDay, "2100-12-31", day("2100-12-31")},
	{colDay, "2024/03/01", day("2024-03-01")},
	{colDay, "03/01/2024", day("2024-03-01")},
	{colDay, "2024-03-01T00:00:00Z", day("2024-03-01")},
	{colDay, "2024-02-29T00:00:00Z", day("2024-02-29")},
	{colAt, "2024-01-15T12:30:00Z", instant("2024-01-15T12:30:00Z")},
	{colAt, "2024-01-15T07:30:00-05:00", instant("2024-01-15T12:30:00Z")},
	{colAt, "2024-01-15T12:30:01Z", instant("2024-01-15T12:30:01Z")},
	{colAt, "2023-12-31T23:59:59Z", instant("2023-12-31T23:59:59Z")},
	{colAt, "2024-12-31T23:59:59Z", instant("2024-12-31T23:59:59Z")},
	{colAt, "2024-03-10T07:00:00Z", instant("2024-03-10T07:00:00Z")},
	{colAt, "2024-03-09T23:00:00-08:00", instant("2024-03-10T07:00:00Z")},
	{colAt, "2024-07-04 09:15:30", instant("2024-07-04T09:15:30Z")},
	{colAt, "2024-07-04T09:15:30", instant("2024-07-04T09:15:30Z")},
	{colAt, "2024-07-04T11:15:30+02:00", instant("2024-07-04T09:15:30Z")},
	{colClock, "00:00:00", clock("00:00:00")},
	{colClock, "09:15:30", clock("09:15:30")},
	{colClock, "12:30", clock("12:30")},
	{colClock, "23:59:59", clock("23:59:59")},
	{colClock, "9:15 AM", clock("09:15")},
	{colClock, "11:59 PM", clock("23:59")},
}

func holds(mode pagination.Mode, key time.Time, ref ...time.Time) bool {
	cmp := key.Compare(ref[0])
	switch mode {
	case pagination.ModeEqual:
		return cmp == 0
	case pagination.ModeNotEqual:
		return cmp != 0
	case pagination.ModeGT, pagination.ModeAfter:
		return cmp > 0
	case pagination.ModeGTE:
		return cmp >= 0
	case pagination.ModeLT, pagination.ModeBefore:
		return cmp < 0
	case pagination.ModeLTE:
		return cmp <= 0
	case pagination.ModeRange:
		return cmp >= 0 && key.Compare(ref[1]) <= 0
	case pagination.ModeInside:
		return slices.ContainsFunc(ref, func(r time.Time) bool { return key.Equal(r) })
	case pagination.ModeOutside:
		return !slices.ContainsFunc(ref, func(r time.Time) bool { return key.Equal(r) })
	}
	panic("no oracle for " + string(mode))
}

// checkEvents runs one filter through every read path and compares each with
// the oracle: Filter, Paginate, Count, Exists, and a cursor walk of 3 rows
// per page, which must visit the same rows once each.
func checkEvents(t *testing.T, fx *eventFixture, flt pagination.Filter, match func(Event) bool) {
	t.Helper()
	var want []string
	for _, e := range eventSeed() {
		if match(e) {
			want = append(want, e.Title)
		}
	}
	titles := func(rows []*Event) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = r.Title
		}
		return out
	}
	ctx := ctxT(t)
	sf := and(flt)

	got, err := fx.svc.Filter(ctx, sf)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if !sameSet(titles(got), want) {
		t.Fatalf("Filter matched %v, want %v", titles(got), want)
	}
	res, err := fx.svc.Paginate(ctx, pagination.Pagination{Filter: sf, PageSize: 50})
	if err != nil {
		t.Fatalf("Paginate: %v", err)
	}
	if !sameSet(titles(res.Data), want) {
		t.Fatalf("Paginate matched %v, want %v", titles(res.Data), want)
	}
	if n, err := fx.svc.Count(ctx, sf); err != nil || int(n) != len(want) {
		t.Fatalf("Count = %d, %v; want %d", n, err, len(want))
	}
	if ok, err := fx.svc.Exists(ctx, sf); err != nil || ok != (len(want) > 0) {
		t.Fatalf("Exists = %v, %v; want %v", ok, err, len(want) > 0)
	}
	walkSF := sf
	walkSF.SortFields = []pagination.SortField{asc("at")}
	var walked []string
	var cursor *string
	for range 100 {
		p, err := fx.svc.Paginate(ctx, pagination.Pagination{Filter: walkSF, PageSize: 3, Cursor: cursor})
		if err != nil {
			t.Fatalf("walk: %v", err)
		}
		walked = append(walked, titles(p.Data)...)
		if cursor = p.NextCursor; cursor == nil {
			break
		}
	}
	if !sameSet(walked, want) || firstDuplicate(walked) != "" {
		t.Fatalf("cursor walk visited %v, want %v once each", walked, want)
	}
}

// checkRejected runs a filter that must be refused by every read path.
func checkRejected(t *testing.T, fx *eventFixture, flt pagination.Filter, wantInvalid bool) {
	t.Helper()
	ctx := ctxT(t)
	_, err1 := fx.svc.Filter(ctx, and(flt))
	_, err2 := fx.svc.Paginate(ctx, pagination.Pagination{Filter: and(flt)})
	for _, err := range []error{err1, err2} {
		if err == nil {
			t.Fatal("expected an error")
		}
		if wantInvalid && !errors.Is(err, pagination.ErrInvalidFilter) {
			t.Fatalf("err = %v, want ErrInvalidFilter", err)
		}
	}
}

func filterOf(p probe, mode pagination.Mode, value any) pagination.Filter {
	return pagination.Filter{Field: p.col.field, Mode: mode, Value: value, DataType: p.col.typ}
}

// Every comparison mode against every probe: 8 modes x 29 values.
func TestMatrix_DateComparisons(t *testing.T) {
	fx := needEvents(t)
	modes := []pagination.Mode{
		pagination.ModeEqual, pagination.ModeNotEqual, pagination.ModeGT, pagination.ModeGTE,
		pagination.ModeLT, pagination.ModeLTE, pagination.ModeBefore, pagination.ModeAfter,
	}
	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			for _, p := range probes {
				t.Run(p.col.field+"/"+p.sent, func(t *testing.T) {
					t.Parallel()
					checkEvents(t, fx, filterOf(p, mode, p.sent), func(e Event) bool {
						return holds(mode, p.col.key(e), p.ref)
					})
				})
			}
		})
	}
}

type rangeProbe struct {
	col      column
	from, to any
	refs     [2]time.Time
}

var rangeProbes = []rangeProbe{
	{colDay, "2024-01-01", "2024-01-31", [2]time.Time{day("2024-01-01"), day("2024-01-31")}},
	{colDay, "2024-02-29", "2024-02-29", [2]time.Time{day("2024-02-29"), day("2024-02-29")}},
	{colDay, "2023-12-31", "2025-01-01", [2]time.Time{day("2023-12-31"), day("2025-01-01")}},
	{colDay, "2024-03-02", "2024-06-29", [2]time.Time{day("2024-03-02"), day("2024-06-29")}},
	{colDay, "2024-06-30", "2024-01-01", [2]time.Time{day("2024-06-30"), day("2024-01-01")}}, // reversed: nothing
	{colDay, "1999-01-01", "2000-01-01", [2]time.Time{day("1999-01-01"), day("2000-01-01")}},
	{colDay, "2024/01/01", "01/31/2024", [2]time.Time{day("2024-01-01"), day("2024-01-31")}}, // mixed layouts
	{colDay, "2024-01-01T00:00:00Z", "2024-03-01T00:00:00Z", [2]time.Time{day("2024-01-01"), day("2024-03-01")}},
	{colDay, "2024-12-31", "2100-01-01", [2]time.Time{day("2024-12-31"), day("2100-01-01")}},
	{colDay, "2024-03-10", "2024-07-04", [2]time.Time{day("2024-03-10"), day("2024-07-04")}},
	{colAt, "2024-01-15T12:30:00Z", "2024-01-31T23:59:59Z", [2]time.Time{instant("2024-01-15T12:30:00Z"), instant("2024-01-31T23:59:59Z")}},
	{colAt, "2024-01-15T07:30:00-05:00", "2024-02-29T03:00:00-05:00", [2]time.Time{instant("2024-01-15T12:30:00Z"), instant("2024-02-29T08:00:00Z")}},
	{colAt, "2024-03-10T07:00:00Z", "2024-03-10T07:00:00Z", [2]time.Time{instant("2024-03-10T07:00:00Z"), instant("2024-03-10T07:00:00Z")}},
	{colAt, "2024-03-10T07:00:01Z", "2024-06-30T18:44:59Z", [2]time.Time{instant("2024-03-10T07:00:01Z"), instant("2024-06-30T18:44:59Z")}},
	{colAt, "2023-01-01T00:00:00Z", "2023-12-31T23:59:58Z", [2]time.Time{instant("2023-01-01T00:00:00Z"), instant("2023-12-31T23:59:58Z")}},
	{colClock, "00:00:00", "09:15:30", [2]time.Time{clock("00:00:00"), clock("09:15:30")}},
	{colClock, "12:00", "23:59:59", [2]time.Time{clock("12:00"), clock("23:59:59")}},
	{colClock, "08:00:00", "08:00:00", [2]time.Time{clock("08:00:00"), clock("08:00:00")}},
	{colClock, "9:00 AM", "6:45 PM", [2]time.Time{clock("09:00"), clock("18:45")}},
	{colClock, "23:59:59", "00:00:00", [2]time.Time{clock("23:59:59"), clock("00:00:00")}}, // wraps midnight: nothing
}

func TestMatrix_DateRange(t *testing.T) {
	fx := needEvents(t)
	for _, rp := range rangeProbes {
		name := fmt.Sprintf("%s/%v..%v", rp.col.field, rp.from, rp.to)
		t.Run(name+"/map", func(t *testing.T) {
			t.Parallel()
			flt := pagination.Filter{Field: rp.col.field, Mode: pagination.ModeRange, DataType: rp.col.typ, Value: map[string]any{"from": rp.from, "to": rp.to}}
			checkEvents(t, fx, flt, func(e Event) bool { return holds(pagination.ModeRange, rp.col.key(e), rp.refs[0], rp.refs[1]) })
		})
	}
	// The typed range, with real time.Time bounds instead of strings.
	typed := []struct {
		name     string
		col      column
		from, to time.Time
	}{
		{"typed day", colDay, day("2024-01-01"), day("2024-03-01")},
		{"typed at", colAt, instant("2024-02-29T08:00:00Z"), instant("2024-07-04T09:15:30Z")},
		{"typed single instant", colAt, instant("2024-01-01T00:00:00Z"), instant("2024-01-01T00:00:00Z")},
		{"typed outside table", colAt, instant("2030-01-01T00:00:00Z"), instant("2031-01-01T00:00:00Z")},
	}
	for _, tc := range typed {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			flt := pagination.Filter{Field: tc.col.field, Mode: pagination.ModeRange, DataType: tc.col.typ, Value: pagination.RangeDate{From: tc.from, To: tc.to}}
			checkEvents(t, fx, flt, func(e Event) bool { return holds(pagination.ModeRange, tc.col.key(e), tc.from, tc.to) })
		})
	}
}

type listProbe struct {
	col  column
	sent []any
	refs []time.Time
}

var listProbes = []listProbe{
	{colDay, []any{"2024-01-01"}, []time.Time{day("2024-01-01")}},
	{colDay, []any{"2024-01-01", "2024-02-29"}, []time.Time{day("2024-01-01"), day("2024-02-29")}},
	{colDay, []any{"2023-12-31", "2024-12-31", "2025-01-01"}, []time.Time{day("2023-12-31"), day("2024-12-31"), day("2025-01-01")}},
	{colDay, []any{"1999-01-01", "2100-01-01"}, []time.Time{day("1999-01-01"), day("2100-01-01")}},
	{colDay, []any{"2024-03-01", "2024-03-01", "2024-03-01"}, []time.Time{day("2024-03-01")}}, // duplicates
	{colDay, []any{"2024-03-01", "2024/06/30", "07/04/2024"}, []time.Time{day("2024-03-01"), day("2024-06-30"), day("2024-07-04")}},
	{colDay, []any{"2024-01-15T00:00:00Z", "2024-10-31"}, []time.Time{day("2024-01-15"), day("2024-10-31")}},
	{colDay, []any{"2024-01-02", "2024-01-03", "2024-01-04"}, []time.Time{day("2024-01-02"), day("2024-01-03"), day("2024-01-04")}},
	{colAt, []any{"2024-01-15T12:30:00Z", "2024-01-15T07:30:00-05:00"}, []time.Time{instant("2024-01-15T12:30:00Z")}},
	{colAt, []any{"2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z"}, []time.Time{instant("2024-01-01T00:00:00Z"), instant("2025-01-01T00:00:00Z")}},
	{colAt, []any{"2024-03-10T07:00:00Z", "2024-03-09T23:00:00-08:00", "2024-06-30T18:45:00Z"}, []time.Time{instant("2024-03-10T07:00:00Z"), instant("2024-06-30T18:45:00Z")}},
	{colAt, []any{"2024-07-04 09:15:30", "2024-10-31T21:00:00Z"}, []time.Time{instant("2024-07-04T09:15:30Z"), instant("2024-10-31T21:00:00Z")}},
	{colAt, []any{"2000-01-01T00:00:00Z"}, []time.Time{instant("2000-01-01T00:00:00Z")}},
	{colClock, []any{"00:00:00"}, []time.Time{clock("00:00:00")}},
	{colClock, []any{"00:00:00", "23:59:59"}, []time.Time{clock("00:00:00"), clock("23:59:59")}},
	{colClock, []any{"9:15 AM", "12:30", "6:45 PM"}, []time.Time{clock("09:15"), clock("12:30"), clock("18:45")}},
	{colClock, []any{"01:02:03"}, []time.Time{clock("01:02:03")}},
}

func TestMatrix_DateInsideOutside(t *testing.T) {
	fx := needEvents(t)
	for _, mode := range []pagination.Mode{pagination.ModeInside, pagination.ModeOutside} {
		t.Run(string(mode), func(t *testing.T) {
			for _, lp := range listProbes {
				t.Run(fmt.Sprintf("%s/%v", lp.col.field, lp.sent), func(t *testing.T) {
					t.Parallel()
					flt := pagination.Filter{Field: lp.col.field, Mode: mode, DataType: lp.col.typ, Value: lp.sent}
					checkEvents(t, fx, flt, func(e Event) bool { return holds(mode, lp.col.key(e), lp.refs...) })
				})
			}
		})
	}
}

// Values that are not dates are refused with ErrInvalidFilter by every mode
// that coerces dates, and by none of them silently.
func TestMatrix_DateRejected(t *testing.T) {
	fx := needEvents(t)
	bad := []string{
		"", " ", "yesterday", "now", "2024-02-30", "2024-13-01", "2024-00-10", "2023-02-29",
		"20240301", "2024-03-01T25:00:00Z", "2024-03-01T10:61:00Z", "1/2", "0000-00-00",
		"2024-03-01; DROP TABLE pg_events", "２０２４-03-01",
	}
	for _, mode := range []pagination.Mode{pagination.ModeEqual, pagination.ModeNotEqual, pagination.ModeGT, pagination.ModeGTE, pagination.ModeLT, pagination.ModeLTE, pagination.ModeBefore, pagination.ModeAfter} {
		for _, v := range bad {
			t.Run(fmt.Sprintf("%s/%q", mode, v), func(t *testing.T) {
				t.Parallel()
				checkRejected(t, fx, pagination.Filter{Field: "day", Mode: mode, Value: v, DataType: pagination.DataTypeDate}, true)
			})
		}
	}
	badClock := []string{"", "24:00:00", "12:60", "99:99", "7", "noon", "12:30:61", "12-30", "1:2:3", "12:30 XM"}
	for _, v := range badClock {
		t.Run(fmt.Sprintf("clock/%q", v), func(t *testing.T) {
			t.Parallel()
			checkRejected(t, fx, pagination.Filter{Field: "clock", Mode: pagination.ModeEqual, Value: v, DataType: pagination.DataTypeTime}, true)
		})
	}
	t.Run("range with one bad bound", func(t *testing.T) {
		t.Parallel()
		checkRejected(t, fx, pagination.Filter{Field: "day", Mode: pagination.ModeRange, DataType: pagination.DataTypeDate, Value: map[string]any{"from": "2024-01-01", "to": "2024-02-30"}}, true)
	})
	t.Run("list with one bad item", func(t *testing.T) {
		t.Parallel()
		checkRejected(t, fx, pagination.Filter{Field: "day", Mode: pagination.ModeInside, DataType: pagination.DataTypeDate, Value: []any{"2024-01-01", "nope", "2024-02-29"}}, true)
	})
}

// A date column is mis-typed: DataType says date but the value is not a
// string. Nothing coerces it, so the database decides; it must still fail
// cleanly rather than match something.
func TestMatrix_DateNonStringValues(t *testing.T) {
	fx := needEvents(t)
	for name, v := range map[string]any{"integer": 20240301, "float": 1.5, "bool": true, "object": map[string]any{"a": 1}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			checkRejected(t, fx, pagination.Filter{Field: "day", Mode: pagination.ModeEqual, Value: v, DataType: pagination.DataTypeDate}, false)
		})
	}
}

// Without a DataType nothing is coerced, so a date string is compared as the
// database parses it (ISO works, other layouts are the database's call).
func TestMatrix_DateWithoutDataType(t *testing.T) {
	fx := needEvents(t)
	iso := func(mode pagination.Mode, v string, ref time.Time) {
		t.Run(string(mode)+"/"+v, func(t *testing.T) {
			t.Parallel()
			flt := pagination.Filter{Field: "day", Mode: mode, Value: v}
			checkEvents(t, fx, flt, func(e Event) bool { return holds(mode, e.Day, ref) })
		})
	}
	iso(pagination.ModeEqual, "2024-03-01", day("2024-03-01"))
	iso(pagination.ModeGT, "2024-06-30", day("2024-06-30"))
	iso(pagination.ModeLTE, "2024-01-31", day("2024-01-31"))
	iso(pagination.ModeBefore, "2024-01-01", day("2024-01-01"))
	iso(pagination.ModeAfter, "2024-12-31", day("2024-12-31"))
	t.Run("a non-date string is a database error", func(t *testing.T) {
		t.Parallel()
		checkRejected(t, fx, pagination.Filter{Field: "day", Mode: pagination.ModeEqual, Value: "not-a-date"}, false)
	})
}

// Dates combined across columns and logic: AND narrows, OR widens, and a
// second filter on another temporal column composes with the first.
func TestMatrix_DateCombinations(t *testing.T) {
	fx := needEvents(t)
	type combo struct {
		name  string
		sf    pagination.StructuredFilter
		match func(Event) bool
	}
	dayF := func(m pagination.Mode, v string) pagination.Filter {
		return pagination.Filter{Field: "day", Mode: m, Value: v, DataType: pagination.DataTypeDate}
	}
	clockF := func(m pagination.Mode, v string) pagination.Filter {
		return pagination.Filter{Field: "clock", Mode: m, Value: v, DataType: pagination.DataTypeTime}
	}
	atF := func(m pagination.Mode, v string) pagination.Filter {
		return pagination.Filter{Field: "at", Mode: m, Value: v, DataType: pagination.DataTypeDate}
	}
	cases := []combo{
		{"january AND morning", and(dayF(pagination.ModeGTE, "2024-01-01"), dayF(pagination.ModeLTE, "2024-01-31"), clockF(pagination.ModeLT, "12:00")),
			func(e Event) bool {
				return !e.Day.Before(day("2024-01-01")) && !e.Day.After(day("2024-01-31")) && clock(e.Clock).Before(clock("12:00"))
			}},
		{"first OR last day", or(dayF(pagination.ModeEqual, "2023-12-31"), dayF(pagination.ModeEqual, "2025-01-01")),
			func(e Event) bool { return e.Day.Equal(day("2023-12-31")) || e.Day.Equal(day("2025-01-01")) }},
		{"last second of any day", and(clockF(pagination.ModeEqual, "23:59:59")),
			func(e Event) bool { return e.Clock == "23:59:59" }},
		{"midnight in 2024", and(clockF(pagination.ModeEqual, "00:00:00"), dayF(pagination.ModeGTE, "2024-01-01"), dayF(pagination.ModeLT, "2025-01-01")),
			func(e Event) bool {
				return e.Clock == "00:00:00" && !e.Day.Before(day("2024-01-01")) && e.Day.Before(day("2025-01-01"))
			}},
		{"day and instant agree", and(dayF(pagination.ModeEqual, "2024-03-10"), atF(pagination.ModeEqual, "2024-03-10T07:00:00Z")),
			func(e Event) bool { return e.Title == "us-dst" }},
		{"day and instant disagree", and(dayF(pagination.ModeEqual, "2024-03-10"), atF(pagination.ModeEqual, "2024-03-01T00:00:00Z")),
			func(e Event) bool { return false }},
		{"before leap day OR after leap day", or(dayF(pagination.ModeBefore, "2024-02-29"), dayF(pagination.ModeAfter, "2024-02-29")),
			func(e Event) bool { return !e.Day.Equal(day("2024-02-29")) }},
		{"second half of 2024", and(atF(pagination.ModeGTE, "2024-07-01T00:00:00Z"), atF(pagination.ModeLT, "2025-01-01T00:00:00Z")),
			func(e Event) bool {
				return !e.At.Before(instant("2024-07-01T00:00:00Z")) && e.At.Before(instant("2025-01-01T00:00:00Z"))
			}},
		{"empty intersection", and(dayF(pagination.ModeLT, "2024-01-01"), dayF(pagination.ModeGT, "2024-12-31")),
			func(e Event) bool { return false }},
		{"every row twice", or(dayF(pagination.ModeLTE, "2024-06-30"), dayF(pagination.ModeGTE, "2024-06-30")),
			func(e Event) bool { return true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var want []string
			for _, e := range eventSeed() {
				if tc.match(e) {
					want = append(want, e.Title)
				}
			}
			got, err := fx.svc.Filter(ctxT(t), tc.sf)
			if err != nil {
				t.Fatal(err)
			}
			titles := make([]string, len(got))
			for i, g := range got {
				titles[i] = g.Title
			}
			if !sameSet(titles, want) {
				t.Fatalf("matched %v, want %v", titles, want)
			}
		})
	}
}
