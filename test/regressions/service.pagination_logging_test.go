package regressions

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/uptrace/bun"
)

// Pagination monitoring: a failure must say which operation failed, on which entity and with what filter
// shape, request mistakes must be told apart from infrastructure failures, and filter values must never
// reach the log.

type pgItem struct {
	bun.BaseModel `bun:"table:pg_items"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	Score         int64  `bun:"score,notnull"`
}

type pgFixture struct {
	e   *sqlEnv
	log *recordingLog
	svc pagination.PaginationServices[pgItem, int64]
}

func newPGFixture(t *testing.T, withLog bool) pgFixture {
	t.Helper()
	e := newSQLEnv(t)
	e.exec(`CREATE TABLE pg_items (id bigserial PRIMARY KEY, name text NOT NULL, score bigint NOT NULL)`)
	e.exec(`INSERT INTO pg_items (name, score) VALUES ('a', 1), ('b', 2), ('c', 3)`)
	db := sqlsvc.NewSQLService(e.dsn, 2, 5, nil, false, nil, nil, nil)
	if err := db.Run(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Stop(bg) })

	f := pgFixture{e: e}
	cfg := pagination.PaginationService[pgItem, int64]{WriteSQLService: db, ColumnDefaultSort: "id DESC"}
	if withLog {
		f.log = &recordingLog{Context: bg}
		cfg.Log = f.log
	}
	f.svc = pagination.NewPaginationService(cfg)
	return f
}

func nameFilter(value string) pagination.StructuredFilter {
	return pagination.StructuredFilter{Filters: []pagination.Filter{
		{Field: "name", Value: value, Mode: pagination.ModeEqual, DataType: pagination.DataTypeText},
	}}
}

func TestPaginationLogging_DroppedFiltersAreWarnedWithTheirField(t *testing.T) {
	f := newPGFixture(t, true)

	res, err := f.svc.Paginate(bg, pagination.Pagination{Filter: pagination.StructuredFilter{Filters: []pagination.Filter{
		{Field: "no_such_column", Value: "x", Mode: pagination.ModeEqual, DataType: pagination.DataTypeText},
	}}})
	if err != nil || len(res.Data) != 3 {
		t.Fatalf("a dropped filter must not fail the query: %d rows, %v", len(res.Data), err)
	}

	w := f.log.await(t, "the dropped filter", withMsg("filter dropped: field is not a column of the entity"))
	if w.level != "warn" || w.fields["field"] != "no_such_column" || w.fields["mode"] != "equal" ||
		w.fields["entity"] != "regressions.pgItem" || w.fields["component"] != "pagination" {
		t.Fatalf("warning = %+v", w)
	}
}

func TestPaginationLogging_RequestMistakesAreWarningsWithAReason(t *testing.T) {
	f := newPGFixture(t, true)
	bad := "not-a-cursor"

	cases := []struct {
		name   string
		run    func() error
		reason string
		check  func(logEvent) bool
	}{
		{"unknown sort field", func() error {
			_, err := f.svc.Paginate(bg, pagination.Pagination{Filter: pagination.StructuredFilter{
				SortFields: []pagination.SortField{{Field: "nope", Order: pagination.SortOrderAsc}},
			}})
			return err
		}, "unknown_field", func(e logEvent) bool { return fmt.Sprint(e.fields["sort"]) == "[nope asc]" }},
		{"garbage cursor", func() error {
			_, err := f.svc.Paginate(bg, pagination.Pagination{PageSize: 5, Cursor: &bad})
			return err
		}, "invalid_cursor", func(e logEvent) bool { return e.fields["has_cursor"] == true && e.fields["page_size"] == 5 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.run(); err == nil {
				t.Fatal("the request should have been rejected")
			}
			e := f.log.await(t, c.name, func(e logEvent) bool { return e.msg == "pagination request rejected" && e.fields["reason"] == c.reason })
			if e.level != "warn" || e.fields["operation"] != "paginate" || e.fields["entity"] != "regressions.pgItem" || !c.check(e) {
				t.Errorf("%s = %+v", c.name, e)
			}
		})
	}
}

func TestPaginationLogging_DatabaseFailuresAreErrorsWithTheirContext(t *testing.T) {
	f := newPGFixture(t, true)
	f.e.exec(`DROP TABLE pg_items`) // the table disappears under the service

	filter := nameFilter("a")
	_, countErr := f.svc.Count(bg, filter)
	_, existsErr := f.svc.Exists(bg, filter)
	_, findErr := f.svc.Find(bg, filter)

	for op, want := range map[string]error{"count": countErr, "exists": existsErr, "paginate": findErr} {
		if want == nil {
			t.Fatalf("%s succeeded against a missing table", op)
		}
		e := f.log.await(t, op, func(e logEvent) bool { return e.msg == "pagination query failed" && e.fields["operation"] == op })
		if e.level != "error" || e.span != "pagination.error" || !errors.Is(e.err, want) {
			t.Errorf("%s = %+v; want an error line carrying the database error", op, e)
		}
		if fmt.Sprint(e.fields["filters"]) != "[name:equal]" || e.fields["entity"] != "regressions.pgItem" {
			t.Errorf("%s fields = %+v; want the filter shape and the entity", op, e.fields)
		}
		if ms, ok := e.fields["duration_ms"].(int64); !ok || ms < 0 {
			t.Errorf("%s duration_ms = %v", op, e.fields["duration_ms"])
		}
	}
}

// Filter values come from users; the database error for a bad one quotes it, but the log must not.
func TestPaginationLogging_FilterValuesNeverReachTheLog(t *testing.T) {
	f := newPGFixture(t, true)
	const secret = "secret@example.com"

	_, err := f.svc.Count(bg, pagination.StructuredFilter{Filters: []pagination.Filter{
		{Field: "score", Value: secret, Mode: pagination.ModeEqual, DataType: pagination.DataTypeNumber},
	}})
	if err == nil || !strings.Contains(err.Error(), secret) {
		t.Fatalf("Count with a non-numeric value = %v; want the database error quoting the value", err)
	}
	if _, err := f.svc.Count(bg, nameFilter(secret)); err != nil {
		t.Fatal(err)
	}

	e := f.log.await(t, "the rejection", func(e logEvent) bool { return e.fields["reason"] == "invalid_value" })
	if e.level != "warn" || e.fields["operation"] != "count" || fmt.Sprint(e.fields["filters"]) != "[score:equal]" {
		t.Errorf("rejection = %+v; want a count warning naming the filter", e)
	}
	for _, e := range f.log.snapshot() {
		if text := fmt.Sprintf("%v %v %v", e.msg, e.err, e.fields); strings.Contains(text, secret) {
			t.Fatalf("a filter value leaked into the log: %+v", e)
		}
	}
}

func TestPaginationLogging_NoLoggerChangesNothing(t *testing.T) {
	f := newPGFixture(t, false)
	f.e.exec(`DROP TABLE pg_items`)

	if _, err := f.svc.Count(bg, nameFilter("a")); err == nil {
		t.Fatal("Count succeeded against a missing table")
	}
	bad := "not-a-cursor"
	if _, err := f.svc.Paginate(bg, pagination.Pagination{Cursor: &bad}); !errors.Is(err, pagination.ErrInvalidCursor) {
		t.Fatalf("Paginate with a bad cursor = %v; want ErrInvalidCursor", err)
	}
}
