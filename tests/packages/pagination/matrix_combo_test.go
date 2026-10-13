//go:build integration

package pagination_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Log is a dense timeline: every six hours for ten days, plus three rows on
// the edge of a second. It carries the same instant three ways (a timestamp,
// its date, its time of day) so date and time filters can be combined on one
// column or across columns.
type Log struct {
	bun.BaseModel `bun:"table:pg_logs"`

	ID        int64     `bun:"id,pk,autoincrement"`
	Title     string    `bun:"title,notnull"`
	At        time.Time `bun:"at,type:timestamptz,notnull"`
	Day       time.Time `bun:"day,type:date,notnull"`
	Clock     string    `bun:"clock,type:time,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

var (
	logs         pagination.IPaginationService[Log, int64]
	logsDBHandle *bun.DB
)

func logsDB() *bun.DB { return logsDBHandle }

func logSeed() []Log {
	row := func(at time.Time) Log {
		return Log{
			Title: at.Format(time.RFC3339Nano), At: at, UpdatedAt: at,
			Day:   time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, time.UTC),
			Clock: at.Format("15:04:05.999999"),
		}
	}
	var out []Log
	for d := 1; d <= 10; d++ {
		for _, h := range []int{0, 6, 12, 18} {
			out = append(out, row(time.Date(2024, 1, d, h, 0, 0, 0, time.UTC)))
		}
	}
	// On the edge of a second, on January 5th.
	out = append(out,
		row(time.Date(2024, 1, 5, 0, 0, 0, 500_000_000, time.UTC)),
		row(time.Date(2024, 1, 5, 12, 0, 0, 1_000, time.UTC)),
		row(time.Date(2024, 1, 5, 23, 59, 59, 999_999_000, time.UTC)),
	)
	return out
}

func setupLogs() (func(), error) {
	s, cleanup, err := openDB((*Log)(nil))
	if err != nil {
		return nil, err
	}
	rows := logSeed()
	ptrs := make([]*Log, len(rows))
	for i := range rows {
		ptrs[i] = &rows[i]
	}
	if _, err := s.Client().NewInsert().Model(&ptrs).Exec(context.Background()); err != nil {
		cleanup()
		return nil, fmt.Errorf("seed logs: %w", err)
	}
	logsDBHandle = s.Client()
	logs = pagination.NewPaginationService(&pagination.PaginationService[Log, int64]{WriteSQLService: s}, utils.NewInflight())
	calCleanup, err := setupCal()
	if err != nil {
		cleanup()
		return nil, err
	}
	return func() { calCleanup(); cleanup() }, nil
}

func needLogs(t testing.TB) pagination.IPaginationService[Log, int64] {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	if logs == nil {
		t.Skip("logs fixture unavailable")
	}
	return logs
}

// checkLogs runs sf through Filter, Paginate, Count, Exists and a 7-row
// cursor walk, and compares each with the oracle over the seed.
func checkLogs(t *testing.T, svc pagination.IPaginationService[Log, int64], sf pagination.StructuredFilter, match func(Log) bool) {
	t.Helper()
	var want []string
	for _, l := range logSeed() {
		if match(l) {
			want = append(want, l.Title)
		}
	}
	titles := func(rows []*Log) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = r.Title
		}
		return out
	}
	ctx := ctxT(t)
	got, err := svc.Filter(ctx, sf)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	// Filter returns at most one default page (30); compare through Paginate for the full set.
	_ = got
	res, err := svc.Paginate(ctx, pagination.Pagination{Filter: sf, PageSize: 500})
	if err != nil {
		t.Fatalf("Paginate: %v", err)
	}
	if !sameSet(titles(res.Data), want) {
		t.Fatalf("Paginate matched %d rows %v, want %d rows %v", len(res.Data), titles(res.Data), len(want), want)
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
		p, err := svc.Paginate(ctx, pagination.Pagination{Filter: walkSF, PageSize: 7, Cursor: cursor})
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

// tod is the time of day of an instant, as a comparable value.
func tod(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(0, 1, 1, u.Hour(), u.Minute(), u.Second(), u.Nanosecond(), time.UTC)
}

// parseTOD reads a time of day the way a client writes it.
func parseTOD(s string) time.Time {
	for _, layout := range []string{"15:04:05.999999999", "15:04:05", "15:04", "3:04:05 PM", "3:04 PM", "3:04PM"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	panic("bad time of day " + s)
}

// spec is one filter (or none) and the rule a row must satisfy for it.
type spec struct {
	name  string
	mk    func(field string) *pagination.Filter
	match func(at time.Time) bool
	timed bool // an instant on a timestamp: not meaningful on a date column
}

func dateF(mode pagination.Mode, v any) func(string) *pagination.Filter {
	return func(field string) *pagination.Filter {
		return &pagination.Filter{Field: field, Mode: mode, Value: v, DataType: pagination.DataTypeDate}
	}
}

func timeF(mode pagination.Mode, v any) func(string) *pagination.Filter {
	return func(field string) *pagination.Filter {
		return &pagination.Filter{Field: field, Mode: mode, Value: v, DataType: pagination.DataTypeTime}
	}
}

func rng(from, to any) map[string]any { return map[string]any{"from": from, "to": to} }

var (
	jan = func(d, h, m, s int) time.Time { return time.Date(2024, 1, d, h, m, s, 0, time.UTC) }
	// endOf is the last instant a row could have on day d.
	nextDay = func(d int) time.Time { return time.Date(2024, 1, d+1, 0, 0, 0, 0, time.UTC) }
)

var dateSpecs = []spec{
	{name: "no date filter", mk: func(string) *pagination.Filter { return nil }, match: func(time.Time) bool { return true }},
	{name: "equal bare day", mk: dateF(pagination.ModeEqual, "2024-01-05"), match: func(a time.Time) bool { return dayWide(pagination.ModeEqual, a, day("2024-01-05")) }},
	{name: "notEqual bare day", mk: dateF(pagination.ModeNotEqual, "2024-01-05"), match: func(a time.Time) bool { return dayWide(pagination.ModeNotEqual, a, day("2024-01-05")) }},
	{name: "gte bare day", mk: dateF(pagination.ModeGTE, "2024-01-04"), match: func(a time.Time) bool { return dayWide(pagination.ModeGTE, a, day("2024-01-04")) }},
	{name: "lt bare day", mk: dateF(pagination.ModeLT, "2024-01-03"), match: func(a time.Time) bool { return dayWide(pagination.ModeLT, a, day("2024-01-03")) }},
	{name: "range bare days", mk: dateF(pagination.ModeRange, rng("2024-01-03", "2024-01-06")), match: func(a time.Time) bool { return !a.Before(jan(3, 0, 0, 0)) && a.Before(nextDay(6)) }},
	{name: "inside bare days", mk: dateF(pagination.ModeInside, []any{"2024-01-02", "2024-01-09"}), match: func(a time.Time) bool {
		return dayWide(pagination.ModeEqual, a, day("2024-01-02")) || dayWide(pagination.ModeEqual, a, day("2024-01-09"))
	}},
	{name: "range bare to timed", timed: true, mk: dateF(pagination.ModeRange, rng("2024-01-03", "2024-01-06T11:59:59Z")), match: func(a time.Time) bool { return !a.Before(jan(3, 0, 0, 0)) && !a.After(jan(6, 11, 59, 59)) }},
	{name: "equal instant", timed: true, mk: dateF(pagination.ModeEqual, "2024-01-05T12:00:00Z"), match: func(a time.Time) bool { return a.Equal(jan(5, 12, 0, 0)) }},
	{name: "gt instant", timed: true, mk: dateF(pagination.ModeGT, "2024-01-05T06:00:00Z"), match: func(a time.Time) bool { return a.After(jan(5, 6, 0, 0)) }},
	{name: "range instants", timed: true, mk: dateF(pagination.ModeRange, rng("2024-01-02T06:00:00Z", "2024-01-04T18:00:00Z")), match: func(a time.Time) bool { return !a.Before(jan(2, 6, 0, 0)) && !a.After(jan(4, 18, 0, 0)) }},
}

func todSpec(name string, mode pagination.Mode, v any, refs ...string) spec {
	rs := make([]time.Time, len(refs))
	for i, r := range refs {
		rs[i] = parseTOD(r)
	}
	return spec{name: name, mk: timeF(mode, v), match: func(a time.Time) bool { return holds(mode, tod(a), rs...) }}
}

var timeSpecs = []spec{
	{name: "no time filter", mk: func(string) *pagination.Filter { return nil }, match: func(time.Time) bool { return true }},
	todSpec("equal noon", pagination.ModeEqual, "12:00", "12:00"),
	todSpec("notEqual midnight", pagination.ModeNotEqual, "00:00", "00:00"),
	todSpec("gte noon", pagination.ModeGTE, "12:00", "12:00"),
	todSpec("gt last second", pagination.ModeGT, "23:59:59", "23:59:59"),
	todSpec("lt 6am", pagination.ModeLT, "06:00", "06:00"),
	todSpec("lte 6pm in 12h", pagination.ModeLTE, "6:00 PM", "6:00 PM"),
	todSpec("equal noon in 12h", pagination.ModeEqual, "12:00 PM", "12:00 PM"),
	todSpec("range 6 to 18", pagination.ModeRange, rng("06:00", "18:00"), "06:00", "18:00"),
	todSpec("range sub-second", pagination.ModeRange, rng("00:00:00.5", "12:00:00.000001"), "00:00:00.5", "12:00:00.000001"),
	todSpec("inside midnight and 6pm", pagination.ModeInside, []any{"00:00", "18:00"}, "00:00", "18:00"),
	todSpec("outside 6am and noon", pagination.ModeOutside, []any{"06:00", "12:00"}, "06:00", "12:00"),
}

func combine(and bool, preds ...func(time.Time) bool) func(time.Time) bool {
	return func(a time.Time) bool {
		if len(preds) == 0 {
			return true
		}
		for _, p := range preds {
			if p(a) != and {
				return !and
			}
		}
		return and
	}
}

// Every date filter x every time-of-day filter, joined with AND and with OR,
// on one timestamp column: "on or after the 4th AND between 06:00 and 18:00",
// "on the 5th OR at noon", and so on, each checked against a Go-side oracle.
func TestCombo_DateAndTimeOnOneTimestampColumn(t *testing.T) {
	svc := needLogs(t)
	runCombos(t, svc, "at", "at", dateSpecs, timeSpecs)
}

// The same combinations with the date on a date column and the time of day
// on a separate time column. Instants only make sense on the timestamp.
func TestCombo_DateColumnAndTimeColumn(t *testing.T) {
	svc := needLogs(t)
	var dates []spec
	for _, s := range dateSpecs {
		if !s.timed {
			dates = append(dates, s)
		}
	}
	runCombos(t, svc, "day", "clock", dates, timeSpecs)
}

func runCombos(t *testing.T, svc pagination.IPaginationService[Log, int64], dateField, timeField string, dates, times []spec) {
	t.Helper()
	for _, joinAnd := range []bool{true, false} {
		join := "AND"
		if !joinAnd {
			join = "OR"
		}
		t.Run(join, func(t *testing.T) {
			for _, d := range dates {
				for _, tm := range times {
					t.Run(d.name+" "+join+" "+tm.name, func(t *testing.T) {
						t.Parallel()
						var fs []pagination.Filter
						var preds []func(time.Time) bool
						if f := d.mk(dateField); f != nil {
							fs = append(fs, *f)
							preds = append(preds, d.match)
						}
						if f := tm.mk(timeField); f != nil {
							fs = append(fs, *f)
							preds = append(preds, tm.match)
						}
						sf := or(fs...)
						if joinAnd {
							sf = and(fs...)
						}
						match := combine(joinAnd, preds...)
						checkLogs(t, svc, sf, func(l Log) bool { return match(l.At) })
					})
				}
			}
		})
	}
}

// Accuracy of time-of-day comparison at the edge of a second, a minute and
// the day, and in the layouts people type.
func TestTimeAccuracy(t *testing.T) {
	svc := needLogs(t)
	cases := []struct {
		name string
		mode pagination.Mode
		v    any
	}{
		{"equal 12:00:00.000001 hits only the microsecond row", pagination.ModeEqual, "12:00:00.000001"},
		{"equal 12:00:00 misses the microsecond row", pagination.ModeEqual, "12:00:00"},
		{"gt 12:00:00 includes the microsecond row", pagination.ModeGT, "12:00:00"},
		{"gt 12:00:00.000001 excludes it", pagination.ModeGT, "12:00:00.000001"},
		{"gte 12:00:00.000001 includes it", pagination.ModeGTE, "12:00:00.000001"},
		{"lt 12:00:00.000001 excludes it", pagination.ModeLT, "12:00:00.000001"},
		{"lte 12:00:00.000001 includes it", pagination.ModeLTE, "12:00:00.000001"},
		{"equal 00:00:00.5", pagination.ModeEqual, "00:00:00.5"},
		{"equal 00:00:00.500000 is the same", pagination.ModeEqual, "00:00:00.500000"},
		{"gt 00:00:00 includes the half second", pagination.ModeGT, "00:00:00"},
		{"lt 00:00:00.5 is exactly midnight", pagination.ModeLT, "00:00:00.5"},
		{"equal 23:59:59.999999", pagination.ModeEqual, "23:59:59.999999"},
		{"gt 23:59:59 includes the last microsecond", pagination.ModeGT, "23:59:59"},
		{"gt 23:59:59.999999 matches nothing", pagination.ModeGT, "23:59:59.999999"},
		{"lte 23:59:59.999999 matches everything", pagination.ModeLTE, "23:59:59.999999"},
		{"gte 00:00:00 matches everything", pagination.ModeGTE, "00:00:00"},
		{"lt 00:00:00 matches nothing", pagination.ModeLT, "00:00:00"},
		{"12:00 AM is midnight", pagination.ModeEqual, "12:00 AM"},
		{"12:00 PM is noon", pagination.ModeEqual, "12:00 PM"},
		{"12:00:00 AM with seconds", pagination.ModeEqual, "12:00:00 AM"},
		{"6:00 PM is 18:00", pagination.ModeEqual, "6:00 PM"},
		{"6:00PM without a space", pagination.ModeEqual, "6:00PM"},
		{"11:59 PM is 23:59", pagination.ModeGTE, "11:59 PM"},
		{"1:00 AM", pagination.ModeGTE, "1:00 AM"},
		{"zero-padded 06:00:00", pagination.ModeEqual, "06:00:00"},
		{"hours and minutes only 18:00", pagination.ModeEqual, "18:00"},
	}
	for _, field := range []string{"at", "clock"} {
		t.Run(field, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					ref := parseTOD(tc.v.(string))
					flt := pagination.Filter{Field: field, Mode: tc.mode, Value: tc.v, DataType: pagination.DataTypeTime}
					checkLogs(t, svc, and(flt), func(l Log) bool { return holds(tc.mode, tod(l.At), ref) })
				})
			}
		})
	}
	// A time of day is compared on its own: the date of the row never matters.
	t.Run("noon matches noon on every day", func(t *testing.T) {
		t.Parallel()
		flt := pagination.Filter{Field: "at", Mode: pagination.ModeEqual, Value: "12:00", DataType: pagination.DataTypeTime}
		res, err := svc.Paginate(ctxT(t), pagination.Pagination{Filter: and(flt), PageSize: 100})
		if err != nil || len(res.Data) != 10 {
			t.Fatalf("matched %d rows, %v; want one noon per day (10)", len(res.Data), err)
		}
	})
	// A range whose start is after its end is empty; it does not wrap past
	// midnight. (Pin this here if overnight ranges are ever added.)
	t.Run("range that would wrap midnight is empty", func(t *testing.T) {
		t.Parallel()
		flt := pagination.Filter{Field: "at", Mode: pagination.ModeRange, Value: rng("22:00", "02:00"), DataType: pagination.DataTypeTime}
		checkLogs(t, svc, and(flt), func(Log) bool { return false })
	})
	t.Run("an overnight window is two ranges joined with OR", func(t *testing.T) {
		t.Parallel()
		sf := or(
			pagination.Filter{Field: "at", Mode: pagination.ModeRange, Value: rng("18:00", "23:59:59.999999"), DataType: pagination.DataTypeTime},
			pagination.Filter{Field: "at", Mode: pagination.ModeRange, Value: rng("00:00", "05:59:59"), DataType: pagination.DataTypeTime},
		)
		checkLogs(t, svc, sf, func(l Log) bool {
			h := l.At.UTC().Hour()
			return h >= 18 || h < 6
		})
	})
}

// Dates on the same dense timeline: the boundary between two days is exact
// to the microsecond.
func TestDateAccuracyAtDayBoundary(t *testing.T) {
	svc := needLogs(t)
	cases := []struct {
		name string
		mode pagination.Mode
		v    string
		day  string
	}{
		{"lte Jan 5 keeps 23:59:59.999999", pagination.ModeLTE, "2024-01-05", "2024-01-05"},
		{"gt Jan 5 starts at Jan 6 00:00", pagination.ModeGT, "2024-01-05", "2024-01-05"},
		{"equal Jan 5 has the three edge rows", pagination.ModeEqual, "2024-01-05", "2024-01-05"},
		{"lt Jan 6 stops before midnight", pagination.ModeLT, "2024-01-06", "2024-01-06"},
		{"gte Jan 6 starts at midnight", pagination.ModeGTE, "2024-01-06", "2024-01-06"},
		{"after Jan 10 is empty", pagination.ModeAfter, "2024-01-10", "2024-01-10"},
		{"before Jan 1 is empty", pagination.ModeBefore, "2024-01-01", "2024-01-01"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ref := day(tc.day)
			flt := pagination.Filter{Field: "at", Mode: tc.mode, Value: tc.v, DataType: pagination.DataTypeDate}
			checkLogs(t, svc, and(flt), func(l Log) bool { return dayWide(tc.mode, l.At, ref) })
		})
	}
	t.Run("last microsecond of Jan 5 as an instant", func(t *testing.T) {
		t.Parallel()
		flt := pagination.Filter{Field: "at", Mode: pagination.ModeLTE, Value: "2024-01-05T23:59:59.999999Z", DataType: pagination.DataTypeDate}
		limit := time.Date(2024, 1, 5, 23, 59, 59, 999_999_000, time.UTC)
		checkLogs(t, svc, and(flt), func(l Log) bool { return !l.At.After(limit) })
	})
	t.Run("one microsecond later is Jan 6", func(t *testing.T) {
		t.Parallel()
		flt := pagination.Filter{Field: "at", Mode: pagination.ModeGT, Value: "2024-01-05T23:59:59.999999Z", DataType: pagination.DataTypeDate}
		limit := time.Date(2024, 1, 5, 23, 59, 59, 999_999_000, time.UTC)
		checkLogs(t, svc, and(flt), func(l Log) bool { return l.At.After(limit) })
	})
}
