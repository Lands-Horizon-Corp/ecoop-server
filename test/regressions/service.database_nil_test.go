package regressions

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
)

// Nil and edge-case guardrails for the database service: NULL columns, missing dependencies,
// zero-row results handed from one call to the next, and nil slices and maps. Every test here must
// end in a value or an error, never a panic; noPanic turns a panic into a test failure that names
// the call instead of crashing the whole run.

type dbProfile struct {
	bun.BaseModel `bun:"table:db_profiles"`
	ID            string         `bun:"id,pk" json:"id"`
	MiddleName    *string        `bun:"middle_name" json:"middle_name"`
	DeletedAt     *time.Time     `bun:"deleted_at" json:"deleted_at"`
	Score         *int64         `bun:"score" json:"score"`
	Metadata      map[string]any `bun:"metadata,type:jsonb,nullzero" json:"metadata"`
	UpdatedAt     time.Time      `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
}

type (
	dbProfileResource struct {
		ID, MiddleName string
		Deleted        bool
	}
	dbProfileRequest struct{}
	dbProfileService = cqrs.CQRSServices[dbProfile, dbProfileResource, dbProfileRequest, string]
)

const dbProfilesMigration = `CREATE TABLE db_profiles (
	id          text PRIMARY KEY,
	middle_name text,
	deleted_at  timestamptz,
	score       bigint,
	metadata    jsonb,
	updated_at  timestamptz NOT NULL DEFAULT now()
);`

// newProfileHarness is newDBHarness plus the all-nullable profiles table. dispatched counts every
// event the profile service dispatches, so tests can prove a nil model never reaches a handler.
func newProfileHarness(t *testing.T, dispatched *atomic.Int64) *dbHarness {
	t.Helper()
	h := newDBHarness(t)
	body := gooseBody(dbProfilesMigration, "DROP TABLE db_profiles;")
	if err := os.WriteFile(filepath.Join("src", "database", "migrations", "00002_db_profiles.sql"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	reg := database.Registration[dbProfile, dbProfileResource, dbProfileRequest, string]{
		Channel: "profiles",
		// A nil-safe serializer: every optional field is checked before it is dereferenced.
		ToResource: func(p *dbProfile) *dbProfileResource {
			r := &dbProfileResource{ID: p.ID, Deleted: p.DeletedAt != nil}
			if p.MiddleName != nil {
				r.MiddleName = *p.MiddleName
			}
			return r
		},
		Created: func(*dbProfile) broadcast.Events { return broadcast.Events{"profile.created"} },
		Updated: func(*dbProfile) broadcast.Events { return broadcast.Events{"profile.updated"} },
		Deleted: func(*dbProfile) broadcast.Events { return broadcast.Events{"profile.deleted"} },
		Dispatch: func(broadcast.Channel, broadcast.Events, *dbProfileResource) error {
			if dispatched != nil {
				dispatched.Add(1)
			}
			return nil
		},
	}
	if err := database.Register(h.svc, reg); err != nil {
		t.Fatalf("Register profile: %v", err)
	}
	return h
}

func (h *dbHarness) startWithProfiles() (dbMemberService, dbProfileService) {
	h.t.Helper()
	members := h.start()
	profiles, err := database.Get[dbProfile, dbProfileResource, dbProfileRequest, string](h.svc)
	if err != nil {
		h.t.Fatalf("Get profile: %v", err)
	}
	return members, profiles
}

// noPanic runs fn and fails the test, naming the call, if it panics.
func noPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", name, r)
		}
	}()
	fn()
}

//go:fix inline
func ptr[T any](v T) *T { return new(v) }

// ---------------------------------------------------------------------------------------------------
// 1. Nullable columns
// ---------------------------------------------------------------------------------------------------

// Happy: NULL columns come back as nil pointers and nil maps on the writer, survive the sync stream as
// JSON null, are still nil on the reader, and the serializer handles them without dereferencing.
func TestDatabaseNil_HappyNullColumnsRoundTripAsNil(t *testing.T) {
	h := newProfileHarness(t, nil)
	_, profiles := h.startWithProfiles()
	h.svc.Run(bg)
	events := h.broker.await(t, "profiles")

	created, err := profiles.Create(bg, dbProfile{ID: "p1"})
	if err != nil {
		t.Fatalf("Create with every optional field nil: %v", err)
	}
	if created.MiddleName != nil || created.DeletedAt != nil || created.Score != nil || created.Metadata != nil {
		t.Fatalf("writer returned %+v; want every optional field nil", created)
	}
	if n := count(t, h.writer, `SELECT count(*) FROM db_profiles WHERE middle_name IS NULL AND deleted_at IS NULL AND score IS NULL AND metadata IS NULL`); n != 1 {
		t.Fatal("writer stored a non-NULL value for a nil field")
	}

	dbPublish(t, events, "p-1", cqrs.ChangeTypeCreated, *created)
	awaitCount(t, h.reader, 1, `SELECT count(*) FROM db_profiles WHERE middle_name IS NULL AND metadata IS NULL`)

	got, err := profiles.GetByID(bg, "p1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.MiddleName != nil || got.DeletedAt != nil || got.Score != nil || got.Metadata != nil {
		t.Fatalf("reader returned %+v; want every optional field nil", got)
	}
	var res *dbProfileResource
	noPanic(t, "GetByIDFormat on a row of NULLs", func() { res, err = profiles.GetByIDFormat(bg, "p1") })
	if err != nil || res == nil || res.MiddleName != "" || res.Deleted {
		t.Fatalf("GetByIDFormat = %+v, %v; want an empty middle name and not deleted", res, err)
	}
}

// Happy: set and NULL values sit side by side; IsEmpty/IsNotEmpty split them, and an empty JSON
// object stays an empty (non-nil) map instead of collapsing to NULL.
func TestDatabaseNil_HappyNullAndSetValuesAreFilteredApart(t *testing.T) {
	h := newProfileHarness(t, nil)
	_, profiles := h.startWithProfiles()

	rows := []dbProfile{
		{ID: "p1"},
		{ID: "p2", MiddleName: new("Lee"), Score: new(int64(7)), DeletedAt: new(time.Now().UTC()), Metadata: map[string]any{}},
		{ID: "p3", MiddleName: new("Ray"), Metadata: map[string]any{"vip": true}},
	}
	if _, err := profiles.CreateMany(bg, rows); err != nil {
		t.Fatalf("CreateMany: %v", err)
	}
	for _, p := range rows {
		if _, err := h.reader.Exec(`INSERT INTO db_profiles (id, middle_name, deleted_at, score, metadata)
			SELECT id, middle_name, deleted_at, score, metadata FROM json_populate_record(NULL::db_profiles, $1)`,
			mustJSON(t, p)); err != nil {
			t.Fatalf("seed reader: %v", err)
		}
	}

	for name, tc := range map[string]struct {
		mode pagination.Mode
		want []string
	}{
		"IsEmpty":    {pagination.ModeIsEmpty, []string{"p1"}},
		"IsNotEmpty": {pagination.ModeIsNotEmpty, []string{"p2", "p3"}},
	} {
		found, err := profiles.Find(bg, pagination.StructuredFilter{
			Filters:    []pagination.Filter{{Field: "middle_name", Mode: tc.mode}},
			SortFields: []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}},
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := profileIDs(found); fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("%s middle_name = %v; want %v", name, got, tc.want)
		}
	}
	p2, err := profiles.GetByID(bg, "p2")
	if err != nil {
		t.Fatal(err)
	}
	if p2.Metadata == nil || len(p2.Metadata) != 0 {
		t.Fatalf("empty JSON object came back as %#v; want a non-nil empty map", p2.Metadata)
	}
}

// Happy: cursor pagination sorted by a nullable column (NULLS LAST) visits every row exactly once,
// including the rows whose sort value is NULL, in both directions.
func TestDatabaseNil_HappyCursorOverNullableSortColumnVisitsEveryRow(t *testing.T) {
	h := newProfileHarness(t, nil)
	_, profiles := h.startWithProfiles()
	for i := 1; i <= 12; i++ {
		var score any
		if i%3 != 0 { // every third row has a NULL score
			score = i * 10
		}
		if _, err := h.reader.Exec(`INSERT INTO db_profiles (id, score) VALUES ($1, $2)`, fmt.Sprintf("p%02d", i), score); err != nil {
			t.Fatal(err)
		}
	}

	page := pagination.Pagination{PageSize: 5, Filter: pagination.StructuredFilter{
		SortFields: []pagination.SortField{{Field: "score", Order: pagination.SortOrderAsc}},
	}}
	seen := map[string]int{}
	var pages []pagination.PaginationResult[dbProfile]
	for range 6 {
		var res pagination.PaginationResult[dbProfile]
		var err error
		noPanic(t, "Paginate over a nullable sort column", func() { res, err = profiles.Paginate(bg, page) })
		if err != nil {
			t.Fatalf("Paginate: %v", err)
		}
		pages = append(pages, res)
		for _, p := range res.Data {
			seen[p.ID]++
		}
		if res.NextCursor == nil {
			break
		}
		page.Cursor = res.NextCursor
	}
	if len(seen) != 12 {
		t.Fatalf("visited %d distinct rows; want 12 (rows with NULL scores lost): %v", len(seen), seen)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("%s visited %d times; want once", id, n)
		}
	}
	if len(pages) > 1 {
		page.Cursor = pages[1].PreviousCursor
		back, err := profiles.Paginate(bg, page)
		if err != nil {
			t.Fatalf("Paginate backward across NULLs: %v", err)
		}
		if fmt.Sprint(profileIDs(back.Data)) != fmt.Sprint(profileIDs(pages[0].Data)) {
			t.Fatalf("backward page = %v; want first page %v", profileIDs(back.Data), profileIDs(pages[0].Data))
		}
	}
}

// ---------------------------------------------------------------------------------------------------
// 2. Nil dependencies
// ---------------------------------------------------------------------------------------------------

// Happy: every optional dependency nil (loggers, broadcaster, broker, validator) still gives a working
// service; the runner without a broker exits with an error instead of panicking, and Stop returns.
func TestDatabaseNil_HappyAllOptionalDependenciesNil(t *testing.T) {
	h := newDBHarness(t)
	svc := database.NewDatabaseService(h.writerDSN, h.readerDSN, 2, 8,
		nil, nil, nil, h.migrations, true, nil, nil, nil, nil, nil, 0, 0)
	if err := database.Register(svc, database.Registration[dbMember, dbMemberResource, dbMemberRequest, string]{}); err != nil {
		t.Fatal(err)
	}
	noPanic(t, "lifecycle with nil dependencies", func() {
		if err := svc.Start(bg); err != nil {
			t.Fatalf("Start: %v", err)
		}
		members, err := database.Get[dbMember, dbMemberResource, dbMemberRequest, string](svc)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if _, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann"}); err != nil {
			t.Errorf("Create: %v", err)
		}
		if _, err := members.Find(bg, pagination.StructuredFilter{}); err != nil {
			t.Errorf("Find: %v", err)
		}
		// No ToResource: the Format variants return nil instead of calling a nil function.
		if res, err := members.CreateFormat(bg, dbMember{ID: "m2", Name: "Bo"}); err != nil || res != nil {
			t.Errorf("CreateFormat without ToResource = %v, %v; want nil, nil", res, err)
		}
		// No FromRequest: the request variants return a typed error.
		if _, err := members.CreateWithValidation(bg, dbMemberRequest{ID: "m3", Name: "Cy"}); !errors.Is(err, cqrs.ErrFromRequestNotSet) {
			t.Errorf("CreateWithValidation without FromRequest = %v; want ErrFromRequestNotSet", err)
		}
		svc.Run(bg) // no broker: the runner returns ErrMessageBrokerNotInitialized
		done := make(chan struct{})
		go func() { svc.Stop(bg); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Stop hung waiting for a runner that has no broker")
		}
	})
}

// Sad: calling the lifecycle out of order (Run or Stop before Start, Stop twice) is harmless.
func TestDatabaseNil_SadLifecycleOutOfOrderDoesNotPanic(t *testing.T) {
	h := newDBHarness(t)
	noPanic(t, "Stop before Start", func() { h.svc.Stop(bg) })
	noPanic(t, "Run before Start", func() { h.svc.Run(bg); h.svc.Stop(bg) })
	noPanic(t, "Writer/Reader before Start", func() {
		if h.svc.Writer() != nil || h.svc.Reader() != nil {
			t.Error("connections exist before Start")
		}
	})
	h.start()
	noPanic(t, "Stop twice", func() { h.svc.Stop(bg); h.svc.Stop(bg) })
}

// Sad: a nil *DatabaseService gives a typed error from Register and Get, not a nil dereference.
func TestDatabaseNil_SadNilServiceReturnsError(t *testing.T) {
	var svc *database.DatabaseService
	noPanic(t, "Register on nil", func() {
		if err := database.Register(svc, dbMemberRegistration()); !errors.Is(err, database.ErrNilService) {
			t.Errorf("Register(nil) = %v; want ErrNilService", err)
		}
	})
	noPanic(t, "Get on nil", func() {
		if _, err := database.Get[dbMember, dbMemberResource, dbMemberRequest, string](svc); !errors.Is(err, database.ErrNilService) {
			t.Errorf("Get(nil) = %v; want ErrNilService", err)
		}
	})
}

// Sad: a handle kept after Stop (its connections are closed and nil) returns errors on every path
// instead of dereferencing the nil client.
func TestDatabaseNil_SadHandleUsedAfterStopReturnsErrors(t *testing.T) {
	h := newDBHarness(t)
	members := h.start()
	h.svc.Stop(bg)

	calls := map[string]func() error{
		"Create":      func() error { return second(members.Create(bg, dbMember{ID: "m1", Name: "Ann"})) },
		"CreateMany":  func() error { return second(members.CreateMany(bg, []dbMember{{ID: "m1", Name: "Ann"}})) },
		"UpdateByID":  func() error { return second(members.UpdateByID(bg, "m1", dbMember{ID: "m1", Name: "Ann"})) },
		"DeleteByID":  func() error { return members.DeleteByID(bg, "m1") },
		"DeleteMany":  func() error { return members.DeleteMany(bg, []string{"m1"}) },
		"IncrementBy": func() error { return second(members.IncrementByID(bg, "m1", "balance", 1)) },
		"StartTx":     func() error { return second(members.StartTx(bg)) },
		"GetByID":     func() error { return second(members.GetByID(bg, "m1")) },
		"Find":        func() error { return second(members.Find(bg, pagination.StructuredFilter{})) },
		"Count":       func() error { return second(members.Count(bg, pagination.StructuredFilter{})) },
		"Paginate":    func() error { return second(members.Paginate(bg, pagination.Pagination{})) },
	}
	for name, call := range calls {
		noPanic(t, name+" after Stop", func() {
			if err := call(); err == nil {
				t.Errorf("%s after Stop returned no error", name)
			}
		})
	}
}

// ---------------------------------------------------------------------------------------------------
// 3. Zero-row results passed to the next call
// ---------------------------------------------------------------------------------------------------

// Sad: lookups that find nothing return (nil, sql.ErrNoRows), and writes aimed at a missing row
// report sql.ErrNoRows rather than a zero-value model.
func TestDatabaseNil_SadZeroRowsReturnErrNoRows(t *testing.T) {
	h := newProfileHarness(t, nil)
	_, profiles := h.startWithProfiles()
	empty := pagination.StructuredFilter{}

	reads := map[string]func() (any, error){
		"GetByID":       func() (any, error) { return profiles.GetByID(bg, "ghost") },
		"GetByIDFormat": func() (any, error) { return profiles.GetByIDFormat(bg, "ghost") },
		"FindOne":       func() (any, error) { return profiles.FindOne(bg, empty) },
		"Max":           func() (any, error) { return profiles.Max(bg, "score", empty) },
		"Min":           func() (any, error) { return profiles.Min(bg, "score", empty) },
		"IncrementByID": func() (any, error) { return profiles.IncrementByID(bg, "ghost", "score", 1) },
		"UpdateByID":    func() (any, error) { return profiles.UpdateByID(bg, "ghost", dbProfile{ID: "ghost"}) },
	}
	for name, read := range reads {
		noPanic(t, name, func() {
			got, err := read()
			if !errors.Is(err, sql.ErrNoRows) {
				t.Errorf("%s on no rows: err = %v; want sql.ErrNoRows", name, err)
			}
			if v := reflect.ValueOf(got); v.IsValid() && !v.IsNil() {
				t.Errorf("%s on no rows returned a non-nil model %+v", name, got)
			}
		})
	}
	if err := profiles.DeleteByID(bg, "ghost"); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("DeleteByID on no rows = %v; want sql.ErrNoRows", err)
	}
}

// Sad: the nil model from a failed lookup, handed to the serializer and event hooks, is ignored:
// no panic, no resource, and no event is dispatched.
func TestDatabaseNil_SadNilModelFromFailedLookupIsIgnoredDownstream(t *testing.T) {
	var dispatched atomic.Int64
	h := newProfileHarness(t, &dispatched)
	_, profiles := h.startWithProfiles()

	missing, err := profiles.GetByID(bg, "ghost")
	if missing != nil || !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetByID = %v, %v; want nil, sql.ErrNoRows", missing, err)
	}
	noPanic(t, "ToModel(nil)", func() {
		if r := profiles.ToModel(missing); r != nil {
			t.Errorf("ToModel(nil) = %+v; want nil", r)
		}
	})
	noPanic(t, "ToModels with nil entries", func() {
		if rs := profiles.ToModels([]*dbProfile{missing, nil}); len(rs) != 0 {
			t.Errorf("ToModels([nil, nil]) = %v; want empty", rs)
		}
	})
	noPanic(t, "event hooks with nil", func() {
		profiles.OnCreated(bg, missing)
		profiles.OnUpdated(bg, missing)
		profiles.OnDeleted(bg, missing)
	})
	// A real model proves the hooks are wired, so a zero count above means nil was skipped.
	profiles.OnCreated(bg, &dbProfile{ID: "p1"})
	deadline := time.Now().Add(2 * time.Second)
	for dispatched.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let any stray nil event land
	if got := dispatched.Load(); got != 1 {
		t.Fatalf("dispatched %d events; want exactly 1 (only the real model)", got)
	}
}

// ---------------------------------------------------------------------------------------------------
// 4. Nil and empty slices and maps
// ---------------------------------------------------------------------------------------------------

// Happy: bulk operations given nil or empty input return an empty, non-nil result and touch nothing;
// reads over an empty table return an empty page rather than nil data or an error.
func TestDatabaseNil_HappyNilAndEmptyBulkInputs(t *testing.T) {
	h := newProfileHarness(t, nil)
	_, profiles := h.startWithProfiles()

	noPanic(t, "bulk calls with nil input", func() {
		for name, call := range map[string]func() (any, error){
			"CreateMany(nil)":               func() (any, error) { return profiles.CreateMany(bg, nil) },
			"CreateMany([])":                func() (any, error) { return profiles.CreateMany(bg, []dbProfile{}) },
			"UpdateMany(nil)":               func() (any, error) { return profiles.UpdateMany(bg, nil) },
			"CreateManyWithValidation(nil)": func() (any, error) { return profiles.CreateManyWithValidation(bg, nil) },
			"CreateManyFormat(nil)":         func() (any, error) { return profiles.CreateManyFormat(bg, nil) },
		} {
			got, err := call()
			if err != nil {
				t.Errorf("%s: %v", name, err)
				continue
			}
			if v := reflect.ValueOf(got); v.IsNil() || v.Len() != 0 {
				t.Errorf("%s = %#v; want an empty, non-nil slice", name, got)
			}
		}
		if err := profiles.DeleteMany(bg, nil); err != nil {
			t.Errorf("DeleteMany(nil): %v", err)
		}
	})
	if n := count(t, h.writer, `SELECT count(*) FROM db_profiles`); n != 0 {
		t.Fatalf("nil bulk input wrote %d rows", n)
	}

	noPanic(t, "reads on an empty table", func() {
		found, err := profiles.Find(bg, pagination.StructuredFilter{Filters: nil, SortFields: nil, Preload: nil})
		if err != nil || len(found) != 0 {
			t.Errorf("Find on empty table = %v, %v; want no rows, no error", found, err)
		}
		page, err := profiles.Paginate(bg, pagination.Pagination{})
		if err != nil || len(page.Data) != 0 || page.NextCursor != nil || page.PreviousCursor != nil {
			t.Errorf("Paginate on empty table = %+v, %v; want an empty page with no cursors", page, err)
		}
		if n, err := profiles.Count(bg, pagination.StructuredFilter{}); err != nil || n != 0 {
			t.Errorf("Count on empty table = %d, %v", n, err)
		}
	})
}

// Sad: filters carrying nil or empty values are rejected with ErrInvalidFilter, never turned into
// broken SQL or a panic.
func TestDatabaseNil_SadNilAndEmptyFilterValues(t *testing.T) {
	h := newProfileHarness(t, nil)
	_, profiles := h.startWithProfiles()

	for name, f := range map[string]pagination.Filter{
		"equal nil":        {Field: "middle_name", Mode: pagination.ModeEqual, Value: nil},
		"inside nil":       {Field: "id", Mode: pagination.ModeInside, Value: nil},
		"inside not list":  {Field: "id", Mode: pagination.ModeInside, Value: "p1"},
		"range nil bounds": {Field: "score", Mode: pagination.ModeRange, Value: map[string]any{"from": nil, "to": nil}},
		"custom nil func":  {Field: "id", Mode: pagination.ModeCustom},
	} {
		noPanic(t, name, func() {
			_, err := profiles.Count(bg, pagination.StructuredFilter{Filters: []pagination.Filter{f}})
			if !errors.Is(err, pagination.ErrInvalidFilter) {
				t.Errorf("%s: Count = %v; want ErrInvalidFilter", name, err)
			}
		})
	}
	noPanic(t, "inside empty list", func() {
		n, err := profiles.Count(bg, pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "id", Mode: pagination.ModeInside, Value: []string{}}}})
		if err != nil || n != 0 {
			t.Errorf("IN over an empty list = %d, %v; want 0 matches and no error", n, err)
		}
	})
}

// Sad: the runner receives messages with null or missing parts: a null payload, a payload of nulls,
// and a payload without an id. It must not panic, must not create a row with an empty primary key,
// and must keep applying the valid message sent after them.
func TestDatabaseNil_SadRunnerIgnoresNullAndIDLessPayloads(t *testing.T) {
	h := newProfileHarness(t, nil)
	h.startWithProfiles()
	h.svc.Run(bg)
	events := h.broker.await(t, "profiles")

	for i, value := range []string{
		`{"event_id":"e1","change_type":1,"payload":null}`,
		`{"event_id":"e2","change_type":1,"payload":{"id":"p1","middle_name":null,"metadata":null,"score":null,"deleted_at":null}}`,
		`{"event_id":"e3","change_type":1,"payload":{"middle_name":"no id"}}`,
		`{"change_type":2}`,
	} {
		noPanic(t, fmt.Sprintf("message %d", i), func() {
			if err := events(fmt.Appendf(nil, "k%d", i), []byte(value)); err != nil {
				t.Errorf("message %d: %v", i, err)
			}
		})
	}
	awaitCount(t, h.reader, 1, `SELECT count(*) FROM db_profiles WHERE id = 'p1' AND metadata IS NULL`)
	time.Sleep(100 * time.Millisecond) // a few flush intervals for the bad messages
	if n := count(t, h.reader, `SELECT count(*) FROM db_profiles WHERE id = ''`); n != 0 {
		t.Fatal("the runner wrote a row with an empty primary key from a payload without an id")
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func profileIDs(rows []*dbProfile) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}
