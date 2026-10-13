//go:build integration

package pagination_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Ledger is a bank's entries. The database stores UTC; the business day an
// entry belongs to is the day in the bank's own zone: Manila's March 10th is
// not Tokyo's, which is not Sydney's.
type Ledger struct {
	bun.BaseModel `bun:"table:pg_ledger"`

	ID        int64     `bun:"id,pk,autoincrement"`
	Ref       string    `bun:"ref,notnull"`
	At        time.Time `bun:"at,type:timestamptz,notnull"`
	Day       time.Time `bun:"day,type:date,notnull"` // the UTC calendar date
	Amount    int64     `bun:"amount,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

var ledger pagination.IPaginationService[Ledger, int64]

// window is one business day, with its first and last-plus-one instants
// written out by hand (not computed by the code under test).
type window struct {
	zone, date  string
	start, next string // RFC3339 UTC: [start, next)
	note        string
}

var windows = []window{
	{"Asia/Manila", "2024-03-10", "2024-03-09T16:00:00Z", "2024-03-10T16:00:00Z", "UTC+8, no daylight saving"},
	{"Asia/Tokyo", "2024-03-10", "2024-03-09T15:00:00Z", "2024-03-10T15:00:00Z", "UTC+9"},
	{"Asia/Shanghai", "2024-03-10", "2024-03-09T16:00:00Z", "2024-03-10T16:00:00Z", "UTC+8, the same day as Manila"},
	{"Asia/Kolkata", "2024-03-10", "2024-03-09T18:30:00Z", "2024-03-10T18:30:00Z", "UTC+5:30"},
	{"Asia/Kathmandu", "2024-03-10", "2024-03-09T18:15:00Z", "2024-03-10T18:15:00Z", "UTC+5:45"},
	{"Pacific/Kiritimati", "2024-03-10", "2024-03-09T10:00:00Z", "2024-03-10T10:00:00Z", "UTC+14, the first place to see the date"},
	{"Pacific/Pago_Pago", "2024-03-10", "2024-03-10T11:00:00Z", "2024-03-11T11:00:00Z", "UTC-11, the last"},
	{"Australia/Sydney", "2024-03-10", "2024-03-09T13:00:00Z", "2024-03-10T13:00:00Z", "AEDT, UTC+11"},
	{"Australia/Sydney", "2024-04-07", "2024-04-06T13:00:00Z", "2024-04-07T14:00:00Z", "daylight saving ends: a 25-hour day"},
	{"Australia/Sydney", "2024-10-06", "2024-10-05T14:00:00Z", "2024-10-06T13:00:00Z", "daylight saving starts: a 23-hour day"},
	{"America/New_York", "2024-03-10", "2024-03-10T05:00:00Z", "2024-03-11T04:00:00Z", "spring forward: a 23-hour day"},
	{"America/New_York", "2024-11-03", "2024-11-03T04:00:00Z", "2024-11-04T05:00:00Z", "fall back: a 25-hour day"},
	{"Europe/London", "2024-03-31", "2024-03-31T00:00:00Z", "2024-03-31T23:00:00Z", "BST starts: 23 hours"},
	{"Europe/London", "2024-10-27", "2024-10-26T23:00:00Z", "2024-10-28T00:00:00Z", "BST ends: 25 hours"},
	{"Pacific/Auckland", "2024-04-07", "2024-04-06T11:00:00Z", "2024-04-07T12:00:00Z", "NZDT ends: 25 hours"},
	{"Asia/Manila", "2024-12-31", "2024-12-30T16:00:00Z", "2024-12-31T16:00:00Z", "year end"},
	{"Asia/Tokyo", "2024-02-29", "2024-02-28T15:00:00Z", "2024-02-29T15:00:00Z", "leap day"},
	{"Asia/Manila", "2024-01-01", "2023-12-31T16:00:00Z", "2024-01-01T16:00:00Z", "new year"},
}

func (w window) startT() time.Time { return instant(w.start) }
func (w window) nextT() time.Time  { return instant(w.next) }

func ledgerSeed() []Ledger {
	seen := map[time.Time]bool{}
	var out []Ledger
	add := func(at time.Time) {
		if seen[at] {
			return
		}
		seen[at] = true
		out = append(out, Ledger{
			Ref: at.Format(time.RFC3339Nano), At: at, UpdatedAt: at, Amount: int64(len(out)) + 1,
			Day: time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, time.UTC),
		})
	}
	for _, w := range windows {
		s, n := w.startT(), w.nextT()
		for _, at := range []time.Time{
			s.Add(-time.Hour), s.Add(-time.Microsecond), s, s.Add(time.Microsecond),
			s.Add(n.Sub(s) / 2), n.Add(-time.Microsecond), n, n.Add(time.Microsecond), n.Add(time.Hour),
		} {
			add(at)
		}
	}
	return out
}

func setupLedger() (func(), error) {
	s, cleanup, err := openDB((*Ledger)(nil))
	if err != nil {
		return nil, err
	}
	rows := ledgerSeed()
	ptrs := make([]*Ledger, len(rows))
	for i := range rows {
		ptrs[i] = &rows[i]
	}
	if _, err := s.Client().NewInsert().Model(&ptrs).Exec(context.Background()); err != nil {
		cleanup()
		return nil, fmt.Errorf("seed ledger: %w", err)
	}
	ledger = pagination.NewPaginationService(&pagination.PaginationService[Ledger, int64]{WriteSQLService: s}, utils.NewInflight())
	return cleanup, nil
}

func needLedger(t testing.TB) pagination.IPaginationService[Ledger, int64] {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	if ledger == nil {
		t.Skip("ledger fixture unavailable")
	}
	return ledger
}

func checkLedger(t *testing.T, sf pagination.StructuredFilter, match func(Ledger) bool) {
	t.Helper()
	checkRows(t, needLedger(t), ledgerSeed(), func(l Ledger) string { return l.Ref }, sf, match)
}

func rejectedLedger(t *testing.T, sf pagination.StructuredFilter) {
	t.Helper()
	svc := needLedger(t)
	_, err1 := svc.Filter(ctxT(t), sf)
	_, err2 := svc.Paginate(ctxT(t), pagination.Pagination{Filter: sf})
	for _, err := range []error{err1, err2} {
		if !errors.Is(err, pagination.ErrInvalidFilter) {
			t.Fatalf("err = %v, want ErrInvalidFilter", err)
		}
	}
}

// inWindow applies a comparison mode to a business day [start, next).
func inWindow(mode pagination.Mode, at, start, next time.Time) bool {
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
	panic("no window oracle for " + string(mode))
}

func zoned(zone string, flt pagination.Filter) pagination.StructuredFilter {
	flt.Timezone = zone
	return and(flt)
}

func atDate(mode pagination.Mode, v any) pagination.Filter {
	return pagination.Filter{Field: "at", Mode: mode, Value: v, DataType: pagination.DataTypeDate}
}

var comparisonModes = []pagination.Mode{
	pagination.ModeEqual, pagination.ModeNotEqual, pagination.ModeGT, pagination.ModeGTE,
	pagination.ModeLT, pagination.ModeLTE, pagination.ModeBefore, pagination.ModeAfter,
}

// "All entries on March 10th in Manila" is from Manila's midnight to the next
// one, to the microsecond, for every zone and every comparison.
func TestBank_BusinessDayInEachZone(t *testing.T) {
	for _, mode := range comparisonModes {
		t.Run(string(mode), func(t *testing.T) {
			for _, w := range windows {
				t.Run(w.zone+"/"+w.date+" ("+w.note+")", func(t *testing.T) {
					t.Parallel()
					s, n := w.startT(), w.nextT()
					checkLedger(t, zoned(w.zone, atDate(mode, w.date)), func(l Ledger) bool { return inWindow(mode, l.At, s, n) })
				})
			}
		})
	}
}

// The day edges of one account: the entry one microsecond before local
// midnight belongs to the day before, the one at midnight to the day.
func TestBank_TheMicrosecondAtMidnight(t *testing.T) {
	for _, w := range windows {
		t.Run(w.zone+"/"+w.date, func(t *testing.T) {
			t.Parallel()
			s, n := w.startT(), w.nextT()
			checkLedger(t, zoned(w.zone, atDate(pagination.ModeEqual, w.date)), func(l Ledger) bool {
				switch l.At {
				case s.Add(-time.Microsecond), n:
					return false
				case s, s.Add(time.Microsecond), n.Add(-time.Microsecond):
					return true
				}
				return !l.At.Before(s) && l.At.Before(n)
			})
		})
	}
}

// The same date means different entries in different banks' zones.
func TestBank_ZoneChangesTheAnswer(t *testing.T) {
	svc := needLedger(t)
	pick := func(zone string) []string {
		res, err := svc.Paginate(ctxT(t), pagination.Pagination{PageSize: 1000, Filter: zoned(zone, atDate(pagination.ModeEqual, "2024-03-10"))})
		if err != nil {
			t.Fatal(err)
		}
		return nameListLedger(res.Data)
	}
	manila, tokyo, sydney, utc, ny := pick("Asia/Manila"), pick("Asia/Tokyo"), pick("Australia/Sydney"), pick(""), pick("America/New_York")
	for name, pair := range map[string][2][]string{
		"Manila vs Tokyo": {manila, tokyo}, "Manila vs Sydney": {manila, sydney}, "Tokyo vs Sydney": {tokyo, sydney},
		"Manila vs UTC": {manila, utc}, "UTC vs New York": {utc, ny}, "Tokyo vs UTC": {tokyo, utc},
	} {
		if sameSet(pair[0], pair[1]) {
			t.Errorf("%s: the same entries for both, but their March 10ths are different days", name)
		}
	}
	// Manila and Shanghai share UTC+8, so they agree.
	if shanghai := pick("Asia/Shanghai"); !sameSet(manila, shanghai) {
		t.Errorf("Manila and Shanghai are both UTC+8 and must agree")
	}
}

func nameListLedger(rows []*Ledger) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Ref
	}
	return out
}

// A fixed offset is a zone too, for places that do not change: "+08:00" is
// Manila, "+05:45" is Kathmandu.
func TestBank_OffsetZones(t *testing.T) {
	cases := []struct{ zone, name string }{
		{"+08:00", "Asia/Manila"}, {"+0800", "Asia/Manila"}, {"+08", "Asia/Manila"}, {"+09:00", "Asia/Tokyo"},
		{"+05:30", "Asia/Kolkata"}, {"+05:45", "Asia/Kathmandu"}, {"+14:00", "Pacific/Kiritimati"},
		{"-11:00", "Pacific/Pago_Pago"}, {"+00:00", ""}, {"Z", ""}, {"UTC", ""},
	}
	for _, tc := range cases {
		t.Run(tc.zone, func(t *testing.T) {
			t.Parallel()
			var w window
			for _, c := range windows {
				if c.zone == tc.name && c.date == "2024-03-10" {
					w = c
				}
			}
			s, n := instant("2024-03-10T00:00:00Z"), instant("2024-03-11T00:00:00Z")
			if tc.name != "" {
				s, n = w.startT(), w.nextT()
			}
			checkLedger(t, zoned(tc.zone, atDate(pagination.ModeEqual, "2024-03-10")), func(l Ledger) bool { return inWindow(pagination.ModeEqual, l.At, s, n) })
		})
	}
}

// A range of bare dates runs from the first day's midnight to the last day's
// next midnight, in the zone, through any daylight-saving change in between.
func TestBank_RangeOfBusinessDays(t *testing.T) {
	cases := []struct {
		name, zone, from, to string
		start, next          string
	}{
		{"Manila two days", "Asia/Manila", "2024-03-10", "2024-03-10", "2024-03-09T16:00:00Z", "2024-03-10T16:00:00Z"},
		{"Sydney across the 25-hour day", "Australia/Sydney", "2024-04-06", "2024-04-07", "2024-04-05T13:00:00Z", "2024-04-07T14:00:00Z"},
		{"Sydney the 23-hour day alone", "Australia/Sydney", "2024-10-06", "2024-10-06", "2024-10-05T14:00:00Z", "2024-10-06T13:00:00Z"},
		{"New York across spring forward", "America/New_York", "2024-03-09", "2024-03-10", "2024-03-09T05:00:00Z", "2024-03-11T04:00:00Z"},
		{"London across the end of BST", "Europe/London", "2024-10-26", "2024-10-27", "2024-10-25T23:00:00Z", "2024-10-28T00:00:00Z"},
		{"Tokyo leap day to march", "Asia/Tokyo", "2024-02-29", "2024-03-01", "2024-02-28T15:00:00Z", "2024-03-01T15:00:00Z"},
		{"Manila new year", "Asia/Manila", "2023-12-31", "2024-01-01", "2023-12-30T16:00:00Z", "2024-01-01T16:00:00Z"},
		{"Kolkata half-hour zone", "Asia/Kolkata", "2024-03-10", "2024-03-10", "2024-03-09T18:30:00Z", "2024-03-10T18:30:00Z"},
		{"Kiritimati and Pago Pago are a day apart in UTC", "Pacific/Pago_Pago", "2024-03-10", "2024-03-10", "2024-03-10T11:00:00Z", "2024-03-11T11:00:00Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, n := instant(tc.start), instant(tc.next)
			flt := atDate(pagination.ModeRange, map[string]any{"from": tc.from, "to": tc.to})
			checkLedger(t, zoned(tc.zone, flt), func(l Ledger) bool { return !l.At.Before(s) && l.At.Before(n) })
		})
	}
}

// A frontend that sends instants with an offset: "from midnight to the last
// instant of the day" must pick the same entries as the bare date, or lose
// entries at the end.
func TestBank_FrontendSendsOffsets(t *testing.T) {
	m := windows[0] // Asia/Manila 2024-03-10
	s, n := m.startT(), m.nextT()
	day := func(l Ledger) bool { return !l.At.Before(s) && l.At.Before(n) }
	cases := []struct {
		name  string
		sf    pagination.StructuredFilter
		match func(Ledger) bool
	}{
		{"gte midnight AND lt next midnight, exact and complete",
			and(atDate(pagination.ModeGTE, "2024-03-10T00:00:00+08:00"), atDate(pagination.ModeLT, "2024-03-11T00:00:00+08:00")), day},
		{"range to the last microsecond",
			and(atDate(pagination.ModeRange, map[string]any{"from": "2024-03-10T00:00:00+08:00", "to": "2024-03-10T23:59:59.999999+08:00"})), day},
		{"range to the last nanosecond is the same, and does not reach the next midnight",
			and(atDate(pagination.ModeRange, map[string]any{"from": "2024-03-10T00:00:00.000000000+08:00", "to": "2024-03-10T23:59:59.999999999+08:00"})), day},
		{"range to the last millisecond loses the final 999 microseconds",
			and(atDate(pagination.ModeRange, map[string]any{"from": "2024-03-10T00:00:00+08:00", "to": "2024-03-10T23:59:59.999+08:00"})),
			func(l Ledger) bool { return day(l) && !l.At.After(n.Add(-time.Millisecond)) }},
		{"range to 23:59:59 loses the last second",
			and(atDate(pagination.ModeRange, map[string]any{"from": "2024-03-10T00:00:00+08:00", "to": "2024-03-10T23:59:59+08:00"})),
			func(l Ledger) bool { return day(l) && !l.At.After(n.Add(-time.Second)) }},
		{"gt one nanosecond before midnight excludes the entry a microsecond before",
			and(atDate(pagination.ModeGT, "2024-03-09T23:59:59.999999999+08:00")), func(l Ledger) bool { return !l.At.Before(s) }},
		{"gte a half microsecond past midnight starts at the next microsecond",
			and(atDate(pagination.ModeGTE, "2024-03-10T00:00:00.0000005+08:00")), func(l Ledger) bool { return l.At.After(s) }},
		{"lt a half microsecond past midnight includes midnight itself",
			and(atDate(pagination.ModeLT, "2024-03-10T00:00:00.0000005+08:00")), func(l Ledger) bool { return !l.At.After(s) }},
		{"equal a half microsecond past midnight matches nothing",
			and(atDate(pagination.ModeEqual, "2024-03-10T00:00:00.0000005+08:00")), func(Ledger) bool { return false }},
		{"the same instant in Tokyo's offset",
			and(atDate(pagination.ModeEqual, "2024-03-10T01:00:00+09:00")), func(l Ledger) bool { return l.At.Equal(s) }},
		{"the same instant in New York's offset",
			and(atDate(pagination.ModeEqual, "2024-03-09T11:00:00-05:00")), func(l Ledger) bool { return l.At.Equal(s) }},
		{"the same instant in UTC",
			and(atDate(pagination.ModeEqual, "2024-03-09T16:00:00Z")), func(l Ledger) bool { return l.At.Equal(s) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); checkLedger(t, tc.sf, tc.match) })
	}
}

// An offset written in the value always wins over the filter's zone; a value
// with no offset is local to it.
func TestBank_ValueOffsetBeatsFilterZone(t *testing.T) {
	m := windows[0]
	s := m.startT() // Manila midnight = 2024-03-09T16:00:00Z
	cases := []struct {
		name, zone, sent string
		utc              string
	}{
		{"naive value, Manila", "Asia/Manila", "2024-03-10 00:00:00", "2024-03-09T16:00:00Z"},
		{"naive value, Tokyo", "Asia/Tokyo", "2024-03-10 01:00:00", "2024-03-09T16:00:00Z"},
		{"naive value, no zone is UTC", "", "2024-03-09 16:00:00", "2024-03-09T16:00:00Z"},
		{"naive with T, Manila", "Asia/Manila", "2024-03-10T00:00:00", "2024-03-09T16:00:00Z"},
		{"naive slash layout, Manila", "Asia/Manila", "2024/03/10 00:00:00", "2024-03-09T16:00:00Z"},
		{"offset in the value, Manila filter", "Asia/Manila", "2024-03-10T01:00:00+09:00", "2024-03-09T16:00:00Z"},
		{"Z in the value, Tokyo filter", "Asia/Tokyo", "2024-03-09T16:00:00Z", "2024-03-09T16:00:00Z"},
		{"offset in the value, New York filter", "America/New_York", "2024-03-09T11:00:00-05:00", "2024-03-09T16:00:00Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := instant(tc.utc)
			if !want.Equal(s) {
				t.Fatal("test table is wrong")
			}
			checkLedger(t, zoned(tc.zone, atDate(pagination.ModeEqual, tc.sent)), func(l Ledger) bool { return l.At.Equal(want) })
		})
	}
}

// A zone on the StructuredFilter is the default; a filter's own zone wins.
func TestBank_ZoneDefaultsAndOverrides(t *testing.T) {
	m, tk := windows[0], windows[1]
	manila := func(l Ledger) bool { return inWindow(pagination.ModeEqual, l.At, m.startT(), m.nextT()) }
	tokyo := func(l Ledger) bool { return inWindow(pagination.ModeEqual, l.At, tk.startT(), tk.nextT()) }
	utc := func(l Ledger) bool {
		return inWindow(pagination.ModeEqual, l.At, instant("2024-03-10T00:00:00Z"), instant("2024-03-11T00:00:00Z"))
	}
	root := func(zone string, fs ...pagination.Filter) pagination.StructuredFilter {
		sf := and(fs...)
		sf.Timezone = zone
		return sf
	}
	own := func(zone string) pagination.Filter {
		f := atDate(pagination.ModeEqual, "2024-03-10")
		f.Timezone = zone
		return f
	}
	t.Run("root zone applies to every filter", func(t *testing.T) {
		t.Parallel()
		checkLedger(t, root("Asia/Manila", atDate(pagination.ModeEqual, "2024-03-10")), manila)
	})
	t.Run("a filter's own zone beats the root's", func(t *testing.T) {
		t.Parallel()
		checkLedger(t, root("Asia/Manila", own("Asia/Tokyo")), tokyo)
	})
	t.Run("a filter without a zone inherits the root, one with a zone keeps it", func(t *testing.T) {
		t.Parallel()
		checkLedger(t, root("Asia/Manila", atDate(pagination.ModeEqual, "2024-03-10"), own("Asia/Manila")), manila)
	})
	t.Run("no zone anywhere is UTC", func(t *testing.T) {
		t.Parallel()
		checkLedger(t, and(atDate(pagination.ModeEqual, "2024-03-10")), utc)
	})
	t.Run("an explicit UTC beats the root", func(t *testing.T) {
		t.Parallel()
		checkLedger(t, root("Asia/Manila", own("UTC")), utc)
	})
	t.Run("OR across two banks' days", func(t *testing.T) {
		t.Parallel()
		sf := or(own("Asia/Manila"), own("Asia/Tokyo"))
		checkLedger(t, sf, func(l Ledger) bool { return manila(l) || tokyo(l) })
	})
	t.Run("AND of two banks' days is where they overlap", func(t *testing.T) {
		t.Parallel()
		sf := and(own("Asia/Manila"), own("Asia/Tokyo"))
		checkLedger(t, sf, func(l Ledger) bool { return manila(l) && tokyo(l) })
	})
	t.Run("a root zone is checked even when the filter is not a date", func(t *testing.T) {
		t.Parallel()
		sf := and(pagination.Filter{Field: "amount", Mode: pagination.ModeGT, Value: 0})
		sf.Timezone = "Mars/Phobos"
		rejectedLedger(t, sf)
	})
}

// Names the library cannot trust are refused rather than guessed.
func TestBank_InvalidZones(t *testing.T) {
	for _, zone := range []string{
		"Mars/Phobos", "Local", "local", "Asia/Nowhere", "+25:00", "-13:00", "+08:99", "UTC+8", "GMT+8",
		"../../etc/passwd", "Asia/Manila; DROP TABLE pg_ledger", "08:00", "++08:00", "EST", "PST",
	} {
		t.Run(zone, func(t *testing.T) {
			t.Parallel()
			rejectedLedger(t, zoned(zone, atDate(pagination.ModeEqual, "2024-03-10")))
		})
	}
	for _, zone := range []string{"", "  ", "UTC", "Z", "gmt"} {
		t.Run(fmt.Sprintf("%q is UTC", zone), func(t *testing.T) {
			t.Parallel()
			checkLedger(t, zoned(zone, atDate(pagination.ModeEqual, "2024-03-10")), func(l Ledger) bool {
				return inWindow(pagination.ModeEqual, l.At, instant("2024-03-10T00:00:00Z"), instant("2024-03-11T00:00:00Z"))
			})
		})
	}
}

// localClock is a row's time of day in a zone, computed by Go's own tzdata, a
// second opinion on what Postgres's AT TIME ZONE says.
func localClock(at time.Time, zone string) time.Time {
	loc, err := time.LoadLocation(zone)
	if err != nil {
		panic(err)
	}
	l := at.In(loc)
	return time.Date(0, 1, 1, l.Hour(), l.Minute(), l.Second(), l.Nanosecond(), time.UTC)
}

// "After 17:00" means the bank's 17:00, not UTC's.
func TestBank_TimeOfDayInTheBanksZone(t *testing.T) {
	zones := []string{"Asia/Manila", "Asia/Tokyo", "Asia/Kolkata", "Asia/Kathmandu", "Australia/Sydney", "America/New_York", "Europe/London", "Pacific/Kiritimati", "Pacific/Pago_Pago", ""}
	modes := []struct {
		mode pagination.Mode
		v    any
		refs []string
	}{
		{pagination.ModeGTE, "17:00", []string{"17:00"}},
		{pagination.ModeLT, "09:00", []string{"09:00"}},
		{pagination.ModeGT, "23:59:59", []string{"23:59:59"}},
		{pagination.ModeEqual, "00:00", []string{"00:00"}},
		{pagination.ModeLTE, "00:00:00.000001", []string{"00:00:00.000001"}},
		{pagination.ModeNotEqual, "12:00", []string{"12:00"}},
		{pagination.ModeRange, map[string]any{"from": "09:00", "to": "17:00"}, []string{"09:00", "17:00"}},
		{pagination.ModeRange, map[string]any{"from": "23:59:59", "to": "23:59:59.999999"}, []string{"23:59:59", "23:59:59.999999"}},
		{pagination.ModeInside, []any{"00:00", "12:00"}, []string{"00:00", "12:00"}},
		{pagination.ModeOutside, []any{"00:00", "12:00"}, []string{"00:00", "12:00"}},
	}
	for _, zone := range zones {
		name := zone
		if name == "" {
			name = "UTC"
		}
		t.Run(name, func(t *testing.T) {
			for _, m := range modes {
				t.Run(fmt.Sprintf("%s/%v", m.mode, m.v), func(t *testing.T) {
					t.Parallel()
					refs := make([]time.Time, len(m.refs))
					for i, r := range m.refs {
						refs[i] = parseTOD(r)
					}
					z := zone
					if z == "" {
						z = "UTC"
					}
					flt := pagination.Filter{Field: "at", Mode: m.mode, Value: m.v, DataType: pagination.DataTypeTime}
					checkLedger(t, zoned(zone, flt), func(l Ledger) bool { return holds(m.mode, localClock(l.At, z), refs...) })
				})
			}
		})
	}
	t.Run("an offset in the value is the zone that value is read in", func(t *testing.T) {
		t.Parallel()
		// "17:00:00+08:00" is the column's clock at +08:00, whatever zone the filter names.
		flt := pagination.Filter{Field: "at", Mode: pagination.ModeGTE, Value: "17:00:00+08:00", DataType: pagination.DataTypeTime}
		ref := parseTOD("17:00")
		for _, zone := range []string{"Asia/Tokyo", "America/New_York", ""} {
			checkLedger(t, zoned(zone, flt), func(l Ledger) bool { return holds(pagination.ModeGTE, localClock(l.At, "Asia/Manila"), ref) })
		}
	})
	t.Run("a fixed-offset zone reads the clock the same way", func(t *testing.T) {
		t.Parallel()
		flt := pagination.Filter{Field: "at", Mode: pagination.ModeGTE, Value: "17:00", DataType: pagination.DataTypeTime}
		ref := parseTOD("17:00")
		checkLedger(t, zoned("+08:00", flt), func(l Ledger) bool { return holds(pagination.ModeGTE, localClock(l.At, "Asia/Manila"), ref) })
	})
	t.Run("a business day and its banking hours together", func(t *testing.T) {
		t.Parallel()
		m := windows[0]
		sf := and(atDate(pagination.ModeEqual, "2024-03-10"), pagination.Filter{Field: "at", Mode: pagination.ModeRange, Value: map[string]any{"from": "09:00", "to": "17:00"}, DataType: pagination.DataTypeTime})
		sf.Timezone = "Asia/Manila"
		lo, hi := parseTOD("09:00"), parseTOD("17:00")
		checkLedger(t, sf, func(l Ledger) bool {
			return inWindow(pagination.ModeEqual, l.At, m.startT(), m.nextT()) && holds(pagination.ModeRange, localClock(l.At, "Asia/Manila"), lo, hi)
		})
	})
}

// A date column has no zone, so a bare date names that calendar day whatever
// zone the filter is read in; only instants shift with the zone.
func TestBank_DateColumnIgnoresTheZone(t *testing.T) {
	for _, zone := range []string{"", "Asia/Manila", "Pacific/Kiritimati", "Pacific/Pago_Pago", "America/New_York", "+14:00", "-11:00"} {
		for _, mode := range comparisonModes {
			t.Run(zone+"/"+string(mode), func(t *testing.T) {
				t.Parallel()
				ref := day("2024-03-10")
				flt := pagination.Filter{Field: "day", Mode: mode, Value: "2024-03-10", DataType: pagination.DataTypeDate}
				checkLedger(t, zoned(zone, flt), func(l Ledger) bool { return holds(mode, l.Day, ref) })
			})
		}
	}
}

// Volume: hundreds of thousands of entries across several days. The business
// day counted through the library must equal the count Postgres gives for the
// same hand-written window, in every zone, and a cursor walk must visit each
// entry once.
func TestBank_VolumeAcrossZones(t *testing.T) {
	if testing.Short() {
		t.Skip("volume")
	}
	s, cleanup, err := openDB((*Ledger)(nil))
	if err != nil {
		t.Skip("no database: ", err)
	}
	defer cleanup()
	db := s.Client()
	ctx := ctxT(t)
	const rows = 300_000
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pg_ledger (ref, at, day, amount, updated_at)
		SELECT 'r' || g, ts, (ts AT TIME ZONE 'UTC')::date, g, ts
		FROM (SELECT g, timestamptz '2024-03-09 00:00:00+00' + g * interval '864 milliseconds' AS ts FROM generate_series(0, ?) g) x`, rows-1); err != nil {
		t.Fatal(err)
	}
	// Entries exactly on the edges of the days under test.
	for _, w := range windows[:3] {
		for _, at := range []time.Time{w.startT().Add(-time.Microsecond), w.startT(), w.nextT().Add(-time.Microsecond), w.nextT()} {
			if _, err := db.NewInsert().Model(&Ledger{Ref: "edge" + at.Format(time.RFC3339Nano), At: at, UpdatedAt: at, Day: day(at.Format("2006-01-02"))}).Exec(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.ExecContext(ctx, "CREATE INDEX pg_ledger_at ON pg_ledger (at); ANALYZE pg_ledger"); err != nil {
		t.Fatal(err)
	}
	svc := pagination.NewPaginationService(&pagination.PaginationService[Ledger, int64]{WriteSQLService: s}, utils.NewInflight())

	for _, w := range windows[:8] {
		t.Run(w.zone+"/"+w.date, func(t *testing.T) {
			var truth int64
			if err := db.NewRaw("SELECT count(*) FROM pg_ledger WHERE at >= ? AND at < ?", w.startT(), w.nextT()).Scan(ctx, &truth); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			got, err := svc.Count(ctx, zoned(w.zone, atDate(pagination.ModeEqual, w.date)))
			if err != nil || got != truth {
				t.Fatalf("Count = %d, %v; the same window in SQL has %d", got, err, truth)
			}
			if took := time.Since(started); took > 5*time.Second {
				t.Fatalf("counting a day of %d entries took %v", got, took)
			}
		})
	}
	t.Run("a cursor walk of one Manila day visits every entry once", func(t *testing.T) {
		w := windows[0]
		var truth int64
		if err := db.NewRaw("SELECT count(*) FROM pg_ledger WHERE at >= ? AND at < ?", w.startT(), w.nextT()).Scan(ctx, &truth); err != nil {
			t.Fatal(err)
		}
		sf := zoned(w.zone, atDate(pagination.ModeEqual, w.date))
		sf.SortFields = []pagination.SortField{asc("at")}
		seen := make(map[int64]bool, truth)
		var cursor *string
		var last time.Time
		for pages := 0; ; pages++ {
			if pages > 200 {
				t.Fatal("walk does not end")
			}
			p, err := svc.Paginate(ctx, pagination.Pagination{Filter: sf, PageSize: 5000, Cursor: cursor})
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range p.Data {
				if seen[r.ID] {
					t.Fatalf("entry %d served twice", r.ID)
				}
				seen[r.ID] = true
				if r.At.Before(w.startT()) || !r.At.Before(w.nextT()) {
					t.Fatalf("entry at %v is outside Manila's March 10th", r.At)
				}
				if r.At.Before(last) {
					t.Fatalf("entries out of order at %v", r.At)
				}
				last = r.At
			}
			if cursor = p.NextCursor; cursor == nil {
				break
			}
		}
		if int64(len(seen)) != truth {
			t.Fatalf("walk visited %d entries, the day has %d", len(seen), truth)
		}
	})
}
