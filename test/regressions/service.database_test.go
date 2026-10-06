package regressions

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
)

// The database service glues three packages together: sql (writer + reader connections and migrations),
// cqrs (writes to the writer, outbox runner syncing the reader) and pagination (reads from the reader).
// These tests drive all three only through database.Register / database.Get against two real Postgres
// databases, with dbBroker standing in for the CDC stream that feeds the outbox runner.

type dbMember struct {
	bun.BaseModel `bun:"table:db_members"`
	ID            string    `bun:"id,pk" json:"id"`
	Name          string    `bun:"name,notnull" json:"name" validate:"required"`
	Balance       int64     `bun:"balance,notnull" json:"balance" validate:"gte=0"`
	UpdatedAt     time.Time `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
}

type (
	dbMemberResource struct{ ID, Name string }
	dbMemberRequest  struct {
		ID   string `validate:"required"`
		Name string `validate:"required"`
	}
	dbMemberService = cqrs.CQRSServices[dbMember, dbMemberResource, dbMemberRequest, string]
)

const dbMembersMigration = `CREATE TABLE db_members (
	id         text PRIMARY KEY,
	name       text NOT NULL,
	balance    bigint NOT NULL DEFAULT 0,
	updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE processed_events (
	event_id   text PRIMARY KEY,
	channel    text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now()
);`

type dbHarness struct {
	t              testing.TB
	svc            *database.DatabaseService
	broker         *dbBroker
	writer, reader *sql.DB // independent inspection connections
	writerDSN      string
	readerDSN      string
	migrations     *os.File
}

func dbMemberRegistration() database.Registration[dbMember, dbMemberResource, dbMemberRequest, string] {
	return database.Registration[dbMember, dbMemberResource, dbMemberRequest, string]{
		Channel:     "members",
		ToResource:  func(m *dbMember) *dbMemberResource { return &dbMemberResource{ID: m.ID, Name: m.Name} },
		FromRequest: func(r *dbMemberRequest) *dbMember { return &dbMember{ID: r.ID, Name: r.Name} },
	}
}

// newDBHarness creates a writer and a reader database sharing one migrations directory and a
// DatabaseService over them with dbMember registered. It does not start the service.
func newDBHarness(t testing.TB) *dbHarness {
	t.Helper()
	_, writerDSN := createTestDatabase(t)
	_, readerDSN := createTestDatabase(t)

	t.Chdir(t.TempDir())
	dir := filepath.Join("src", "database", "migrations")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := gooseBody(dbMembersMigration, "DROP TABLE processed_events;\nDROP TABLE db_members;")
	if err := os.WriteFile(filepath.Join(dir, "00001_db_members.sql"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	migrations, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = migrations.Close() })

	h := &dbHarness{
		t:          t,
		broker:     &dbBroker{handlers: map[string]func(key, value []byte) error{}},
		writerDSN:  writerDSN,
		readerDSN:  readerDSN,
		migrations: migrations,
	}
	h.svc = h.newService(writerDSN, readerDSN)
	if err := database.Register(h.svc, dbMemberRegistration()); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h.writer = openInspect(t, writerDSN)
	h.reader = openInspect(t, readerDSN)
	return h
}

func (h *dbHarness) newService(writerDSN, readerDSN string) *database.DatabaseService {
	return database.NewDatabaseService(
		writerDSN, readerDSN,
		2, 8,
		nil, nil, nil,
		h.migrations, true, io.Discard, nil,
		nil, h.broker, nil,
		1, 20*time.Millisecond,
	)
}

func openInspect(t testing.TB, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open inspection connection: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// start starts the service and returns the typed member service; Stop runs at cleanup.
func (h *dbHarness) start() dbMemberService {
	h.t.Helper()
	if err := h.svc.Start(bg); err != nil {
		h.t.Fatalf("Start: %v", err)
	}
	h.t.Cleanup(func() { h.svc.Stop(bg) })
	members, err := database.Get[dbMember, dbMemberResource, dbMemberRequest, string](h.svc)
	if err != nil {
		h.t.Fatalf("Get: %v", err)
	}
	return members
}

// run starts the outbox runners and returns the members handler they subscribed.
func (h *dbHarness) run() func(key, value []byte) error {
	h.t.Helper()
	h.svc.Run(bg)
	return h.broker.await(h.t, "members")
}

// publish delivers a member change the way the CDC stream would.
func (h *dbHarness) publish(handler func(key, value []byte) error, eventID string, change cqrs.ChangeType, m dbMember) {
	h.t.Helper()
	dbPublish(h.t, handler, eventID, change, m)
}

func dbPublish[T any](t testing.TB, handler func(key, value []byte) error, eventID string, change cqrs.ChangeType, payload T) {
	t.Helper()
	value, err := json.Marshal(cqrs.CQRSQueuePayload[T]{EventID: eventID, ChangeType: change, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if err := handler([]byte(eventID), value); err != nil {
		t.Fatalf("broker handler: %v", err)
	}
}

// dbBroker is an in-memory broker that keeps one subscriber per channel, standing in for the CDC
// stream; each registered model's runner subscribes to its own channel.
type dbBroker struct {
	mu       sync.Mutex
	handlers map[string]func(key, value []byte) error
}

func (*dbBroker) Run(context.Context) error                             { return nil }
func (*dbBroker) Stop(context.Context) error                            { return nil }
func (*dbBroker) Publish(context.Context, string, []byte, []byte) error { return nil }
func (b *dbBroker) Subscribe(ctx context.Context, channel string, h func(key, value []byte) error) error {
	b.mu.Lock()
	b.handlers[channel] = h
	b.mu.Unlock()
	<-ctx.Done()
	return nil
}

// await returns the handler subscribed on channel, failing if no runner subscribes in time.
func (b *dbBroker) await(t testing.TB, channel string) func(key, value []byte) error {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		h := b.handlers[channel]
		b.mu.Unlock()
		if h != nil {
			return h
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no runner subscribed to %q", channel)
	return nil
}

func count(t testing.TB, db *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

// awaitCount polls until query returns want, failing after a few seconds.
func awaitCount(t testing.TB, db *sql.DB, want int64, query string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := count(t, db, query, args...)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%q = %d; want %d", query, got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// seedReader writes n members straight into the read model: m01..mNN with balance i*100.
func (h *dbHarness) seedReader(n int) {
	h.t.Helper()
	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("member %02d", i)
		if i%5 == 0 {
			name = fmt.Sprintf("vip %02d", i)
		}
		if _, err := h.reader.Exec(`INSERT INTO db_members (id, name, balance) VALUES ($1, $2, $3)`,
			fmt.Sprintf("m%02d", i), name, i*100); err != nil {
			h.t.Fatalf("seed reader: %v", err)
		}
	}
}

func ids(rows []*dbMember) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

// ---------------------------------------------------------------------------------------------------
// sql: connections, migrations and the registry lifecycle
// ---------------------------------------------------------------------------------------------------

// Smoke: Start migrates both databases, both connections answer, and the registered model is served.
func TestDatabaseSQL_SmokeStartMigratesWriterAndReader(t *testing.T) {
	h := newDBHarness(t)
	h.start()

	for name, db := range map[string]*sql.DB{"writer": h.writer, "reader": h.reader} {
		for _, table := range []string{"db_members", "processed_events"} {
			if count(t, db, `SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, table) != 1 {
				t.Errorf("%s database has no %s table after Start", name, table)
			}
		}
	}
	if err := h.svc.Writer().Ping(bg); err != nil {
		t.Errorf("Writer().Ping: %v", err)
	}
	if err := h.svc.Reader().Ping(bg); err != nil {
		t.Errorf("Reader().Ping: %v", err)
	}
}

// Happy: the writer and reader are separate databases; a write lands on the writer only.
func TestDatabaseSQL_HappyWriterAndReaderAreSeparate(t *testing.T) {
	h := newDBHarness(t)
	members := h.start()

	if _, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann", Balance: 10}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := count(t, h.writer, `SELECT count(*) FROM db_members`); got != 1 {
		t.Fatalf("writer has %d members; want 1", got)
	}
	if got := count(t, h.reader, `SELECT count(*) FROM db_members`); got != 0 {
		t.Fatalf("reader has %d members before any sync; want 0", got)
	}
}

// Sad: an unreachable writer fails Start, and nothing can be fetched from the registry.
func TestDatabaseSQL_SadUnreachableWriterFailsStart(t *testing.T) {
	h := newDBHarness(t)
	svc := h.newService(fmt.Sprintf("postgres://x:y@%s/none?sslmode=disable&connect_timeout=2", closedAddr(t)), h.readerDSN)
	if err := database.Register(svc, dbMemberRegistration()); err != nil {
		t.Fatal(err)
	}

	err := svc.Start(bg)
	if err == nil {
		svc.Stop(bg)
		t.Fatal("Start succeeded with an unreachable writer")
	}
	if _, err := database.Get[dbMember, dbMemberResource, dbMemberRequest, string](svc); !errors.Is(err, database.ErrNotStarted) {
		t.Fatalf("Get after a failed Start = %v; want ErrNotStarted", err)
	}
}

// Sad: an unreachable reader fails Start and closes the writer it had already opened.
func TestDatabaseSQL_SadUnreachableReaderFailsStartAndClosesWriter(t *testing.T) {
	h := newDBHarness(t)
	svc := h.newService(h.writerDSN, fmt.Sprintf("postgres://x:y@%s/none?sslmode=disable&connect_timeout=2", closedAddr(t)))

	if err := svc.Start(bg); err == nil {
		svc.Stop(bg)
		t.Fatal("Start succeeded with an unreachable reader")
	}
	if err := svc.Writer().Ping(bg); err == nil {
		t.Fatal("the writer is still open after Start failed on the reader")
	}
}

// Sad: the registry rejects late, duplicate, unknown and mistyped models, and Stop closes it again.
func TestDatabaseSQL_SadRegistryLifecycleErrors(t *testing.T) {
	h := newDBHarness(t)

	if err := database.Register(h.svc, dbMemberRegistration()); !errors.Is(err, database.ErrAlreadyRegistered) {
		t.Errorf("duplicate Register = %v; want ErrAlreadyRegistered", err)
	}
	h.start()
	if err := database.Register(h.svc, database.Registration[cqAccount, cqResource, cqRequest, string]{}); !errors.Is(err, database.ErrAlreadyStarted) {
		t.Errorf("Register after Start = %v; want ErrAlreadyStarted", err)
	}
	if _, err := database.Get[cqAccount, cqResource, cqRequest, string](h.svc); !errors.Is(err, database.ErrNotRegistered) {
		t.Errorf("Get of an unregistered model = %v; want ErrNotRegistered", err)
	}
	if _, err := database.Get[dbMember, dbMember, dbMemberRequest, string](h.svc); !errors.Is(err, database.ErrTypeMismatch) {
		t.Errorf("Get with other type parameters = %v; want ErrTypeMismatch", err)
	}
	h.svc.Stop(bg)
	if _, err := database.Get[dbMember, dbMemberResource, dbMemberRequest, string](h.svc); !errors.Is(err, database.ErrNotStarted) {
		t.Errorf("Get after Stop = %v; want ErrNotStarted", err)
	}
}

// ---------------------------------------------------------------------------------------------------
// cqrs: writes on the writer, the outbox runner syncing the reader
// ---------------------------------------------------------------------------------------------------

// Smoke: the full round trip. Create on the writer, the change arrives from the broker, the runner
// syncs it into the reader, and GetByID (served by pagination from the reader) returns it.
func TestDatabaseCQRS_SmokeWriteSyncsToReader(t *testing.T) {
	h := newDBHarness(t)
	members := h.start()
	handler := h.run()

	created, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann", Balance: 50})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	h.publish(handler, "evt-1", cqrs.ChangeTypeCreated, *created)
	awaitCount(t, h.reader, 1, `SELECT count(*) FROM db_members WHERE id = 'm1'`)

	got, err := members.GetByID(bg, "m1")
	if err != nil {
		t.Fatalf("GetByID after sync: %v", err)
	}
	if got.Name != "Ann" || got.Balance != 50 {
		t.Fatalf("GetByID = %+v; want Ann with balance 50", got)
	}
}

// Happy: requests go through validation and FromRequest, and the Format variants apply ToResource.
func TestDatabaseCQRS_HappyRequestAndFormatMapping(t *testing.T) {
	h := newDBHarness(t)
	members := h.start()

	res, err := members.CreateWithValidationFormat(bg, dbMemberRequest{ID: "m1", Name: "Ann"})
	if err != nil {
		t.Fatalf("CreateWithValidationFormat: %v", err)
	}
	if res == nil || res.ID != "m1" || res.Name != "Ann" {
		t.Fatalf("resource = %+v; want {m1 Ann}", res)
	}
	updated, err := members.UpdateByIDFormat(bg, "m1", dbMember{ID: "m1", Name: "Annie", Balance: 5})
	if err != nil {
		t.Fatalf("UpdateByIDFormat: %v", err)
	}
	if updated.Name != "Annie" {
		t.Fatalf("updated resource = %+v; want Annie", updated)
	}
	if got := count(t, h.writer, `SELECT balance FROM db_members WHERE id = 'm1'`); got != 5 {
		t.Fatalf("writer balance = %d; want 5", got)
	}
}

// Happy: a redelivered event is applied once, and a delete event removes the row from the reader.
func TestDatabaseCQRS_HappyRedeliveryIsIdempotentAndDeleteSyncs(t *testing.T) {
	h := newDBHarness(t)
	h.start()
	handler := h.run()

	m := dbMember{ID: "m1", Name: "Ann", Balance: 1, UpdatedAt: time.Now()}
	h.publish(handler, "evt-1", cqrs.ChangeTypeCreated, m)
	awaitCount(t, h.reader, 1, `SELECT count(*) FROM processed_events WHERE event_id = 'evt-1'`)

	m.Name = "changed by a replay"
	h.publish(handler, "evt-1", cqrs.ChangeTypeCreated, m) // broker redelivery of the same event
	h.publish(handler, "evt-2", cqrs.ChangeTypeDeleted, m)
	awaitCount(t, h.reader, 0, `SELECT count(*) FROM db_members`)

	if got := count(t, h.reader, `SELECT count(*) FROM processed_events`); got != 2 {
		t.Fatalf("processed_events has %d rows; want 2 (the replay must not be recorded twice)", got)
	}
}

// Sad: invalid data is rejected by the validator before anything reaches the writer.
func TestDatabaseCQRS_SadValidationRejectsBeforeWriting(t *testing.T) {
	h := newDBHarness(t)
	members := h.start()

	if _, err := members.Create(bg, dbMember{ID: "m1", Name: ""}); err == nil {
		t.Error("Create accepted a member with no name")
	}
	if _, err := members.Create(bg, dbMember{ID: "m2", Name: "Neg", Balance: -1}); err == nil {
		t.Error("Create accepted a negative balance")
	}
	if _, err := members.CreateWithValidation(bg, dbMemberRequest{ID: "m3"}); err == nil {
		t.Error("CreateWithValidation accepted a request with no name")
	}
	if got := count(t, h.writer, `SELECT count(*) FROM db_members`); got != 0 {
		t.Fatalf("writer has %d members after only invalid writes; want 0", got)
	}
}

// Sad: missing rows report sql.ErrNoRows, and a transaction ended with an error rolls back.
func TestDatabaseCQRS_SadMissingRowsAndRolledBackTransaction(t *testing.T) {
	h := newDBHarness(t)
	members := h.start()

	if _, err := members.UpdateByID(bg, "ghost", dbMember{ID: "ghost", Name: "x"}); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("UpdateByID of a missing id = %v; want sql.ErrNoRows", err)
	}
	if err := members.DeleteByID(bg, "ghost"); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("DeleteByID of a missing id = %v; want sql.ErrNoRows", err)
	}

	tx, err := members.StartTx(bg)
	if err != nil {
		t.Fatalf("StartTx: %v", err)
	}
	if _, err := members.CreateWithTx(bg, tx, dbMember{ID: "m1", Name: "Ann"}); err != nil {
		t.Fatalf("CreateWithTx: %v", err)
	}
	failure := errors.New("later step failed")
	if err := members.EndTx(bg, tx, failure); !errors.Is(err, failure) {
		t.Errorf("EndTx = %v; want the original failure back", err)
	}
	if got := count(t, h.writer, `SELECT count(*) FROM db_members`); got != 0 {
		t.Fatalf("writer has %d members after a rolled-back transaction; want 0", got)
	}
}

// ---------------------------------------------------------------------------------------------------
// pagination: reads served from the reader
// ---------------------------------------------------------------------------------------------------

// Smoke: walking every page with the cursor visits each row exactly once, in order, then stops.
func TestDatabasePagination_SmokeCursorWalksEveryRowOnce(t *testing.T) {
	h := newDBHarness(t)
	members := h.start()
	h.seedReader(25)

	page := pagination.Pagination{
		PageSize: 10,
		Filter:   pagination.StructuredFilter{SortFields: []pagination.SortField{{Field: "balance", Order: pagination.SortOrderAsc}}},
	}
	var seen []string
	var sizes []int
	for range 5 {
		res, err := members.Paginate(bg, page)
		if err != nil {
			t.Fatalf("Paginate: %v", err)
		}
		sizes = append(sizes, len(res.Data))
		seen = append(seen, ids(res.Data)...)
		if res.NextCursor == nil {
			break
		}
		page.Cursor = res.NextCursor
	}
	if fmt.Sprint(sizes) != "[10 10 5]" {
		t.Fatalf("page sizes = %v; want [10 10 5]", sizes)
	}
	for i, id := range seen {
		if want := fmt.Sprintf("m%02d", i+1); id != want {
			t.Fatalf("row %d = %s; want %s (rows: %v)", i, id, want, seen)
		}
	}
}

// Happy: filters, OR logic, Count and Exists agree with each other.
func TestDatabasePagination_HappyFiltersCountAndExistsAgree(t *testing.T) {
	h := newDBHarness(t)
	members := h.start()
	h.seedReader(25)

	vips := pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "name", Mode: pagination.ModeStartsWith, Value: "vip"}}}
	rich := pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "balance", Mode: pagination.ModeGTE, Value: 2100, DataType: pagination.DataTypeNumber}}}
	either := pagination.StructuredFilter{Logic: pagination.LogicOr, Filters: append(vips.Filters, rich.Filters...)}

	for name, tc := range map[string]struct {
		filter pagination.StructuredFilter
		want   int64
	}{
		"vips":   {vips, 5},   // m05 m10 m15 m20 m25
		"rich":   {rich, 5},   // m21..m25
		"either": {either, 9}, // the union; m25 is in both
	} {
		found, err := members.Find(bg, tc.filter)
		if err != nil {
			t.Fatalf("%s: Find: %v", name, err)
		}
		n, err := members.Count(bg, tc.filter)
		if err != nil {
			t.Fatalf("%s: Count: %v", name, err)
		}
		if int64(len(found)) != tc.want || n != tc.want {
			t.Errorf("%s: Find returned %d and Count %d; want %d", name, len(found), n, tc.want)
		}
		if ok, err := members.Exists(bg, tc.filter); err != nil || !ok {
			t.Errorf("%s: Exists = %v, %v; want true", name, ok, err)
		}
	}
	none := pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "name", Mode: pagination.ModeEqual, Value: "nobody"}}}
	if ok, err := members.Exists(bg, none); err != nil || ok {
		t.Errorf("Exists(nobody) = %v, %v; want false", ok, err)
	}
}

// Happy: Max/Min and the backward cursor return the right rows.
func TestDatabasePagination_HappyExtremesAndPreviousPage(t *testing.T) {
	h := newDBHarness(t)
	members := h.start()
	h.seedReader(25)

	richest, err := members.Max(bg, "balance", pagination.StructuredFilter{})
	if err != nil || richest.ID != "m25" {
		t.Fatalf("Max(balance) = %+v, %v; want m25", richest, err)
	}
	poorest, err := members.Min(bg, "balance", pagination.StructuredFilter{})
	if err != nil || poorest.ID != "m01" {
		t.Fatalf("Min(balance) = %+v, %v; want m01", poorest, err)
	}

	page := pagination.Pagination{
		PageSize: 5,
		Filter:   pagination.StructuredFilter{SortFields: []pagination.SortField{{Field: "balance", Order: pagination.SortOrderAsc}}},
	}
	first, err := members.Paginate(bg, page)
	if err != nil {
		t.Fatal(err)
	}
	page.Cursor = first.NextCursor
	second, err := members.Paginate(bg, page)
	if err != nil {
		t.Fatal(err)
	}
	if second.PreviousCursor == nil {
		t.Fatal("the second page has no PreviousCursor")
	}
	page.Cursor = second.PreviousCursor
	back, err := members.Paginate(bg, page)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ids(back.Data)) != fmt.Sprint(ids(first.Data)) {
		t.Fatalf("going back from page 2 gave %v; want page 1 %v", ids(back.Data), ids(first.Data))
	}
}

// Sad: a forged cursor and an unknown sort column are rejected, not silently ignored.
func TestDatabasePagination_SadBadCursorAndUnknownSortField(t *testing.T) {
	h := newDBHarness(t)
	members := h.start()
	h.seedReader(3)

	forged := "not-a-real-cursor"
	if _, err := members.Paginate(bg, pagination.Pagination{PageSize: 2, Cursor: &forged}); !errors.Is(err, pagination.ErrInvalidCursor) {
		t.Errorf("Paginate with a forged cursor = %v; want ErrInvalidCursor", err)
	}
	unknownSort := pagination.Pagination{Filter: pagination.StructuredFilter{SortFields: []pagination.SortField{{Field: "password", Order: pagination.SortOrderAsc}}}}
	if _, err := members.Paginate(bg, unknownSort); !errors.Is(err, pagination.ErrUnknownField) {
		t.Errorf("Paginate sorted by an unknown column = %v; want ErrUnknownField", err)
	}
	unknownFilter := pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "password", Mode: pagination.ModeEqual, Value: "x"}}}
	if _, err := members.Count(bg, unknownFilter); !errors.Is(err, pagination.ErrUnknownField) {
		t.Errorf("Count filtered by an unknown column = %v; want ErrUnknownField", err)
	}
}

// Sad: reads come from the reader, so a row that exists only on the writer is not visible until
// the outbox syncs it. This pins the read-after-write gap callers must design around.
func TestDatabasePagination_SadReadsDoNotSeeUnsyncedWrites(t *testing.T) {
	h := newDBHarness(t)
	members := h.start()

	if _, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := members.GetByID(bg, "m1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetByID before sync = %v; want sql.ErrNoRows from the reader", err)
	}
	if n, err := members.Count(bg, pagination.StructuredFilter{}); err != nil || n != 0 {
		t.Fatalf("Count before sync = %d, %v; want 0 from the reader", n, err)
	}
}
