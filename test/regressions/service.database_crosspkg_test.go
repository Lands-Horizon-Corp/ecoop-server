package regressions

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
)

// Cross-package tests: sql, cqrs and pagination sharing database state through the DatabaseService.
// They cover four areas: one transaction spanning all three packages, a sequential pipeline that must
// never read stale state, concurrent writers on the same rows, and values that must survive every
// layer unchanged (foreign keys, a Postgres enum, JSONB and database defaults).
// Run with -race: several tests drive 50-100 goroutines through the same rows.

type dbLedger struct {
	bun.BaseModel `bun:"table:db_ledgers"`
	ID            string         `bun:"id,pk" json:"id"`
	MemberID      string         `bun:"member_id,notnull" json:"member_id" validate:"required"`
	Kind          string         `bun:"kind,type:ledger_kind,notnull" json:"kind"`
	Amount        int64          `bun:"amount,notnull" json:"amount"`
	Meta          map[string]any `bun:"meta,type:jsonb" json:"meta"`
	Status        string         `bun:"status,nullzero,notnull,default:'posted'" json:"status"`
	CreatedAt     time.Time      `bun:"created_at,nullzero,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt     time.Time      `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
}

type (
	dbLedgerResource struct {
		ID, Kind, Status string
		Meta             map[string]any
	}
	dbLedgerRequest struct{}
	dbLedgerService = cqrs.CQRSServices[dbLedger, dbLedgerResource, dbLedgerRequest, string]
)

// The enum is enforced by Postgres only (no validate tag), so the boundary tests reach the database.
const dbLedgersMigration = `CREATE TYPE ledger_kind AS ENUM ('credit', 'debit');
CREATE TABLE db_ledgers (
	id         text PRIMARY KEY,
	member_id  text NOT NULL REFERENCES db_members (id),
	kind       ledger_kind NOT NULL,
	amount     bigint NOT NULL CHECK (amount > 0),
	meta       jsonb,
	status     text NOT NULL DEFAULT 'posted',
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now()
);`

// newLedgerHarness is newDBHarness plus the ledger table (child of db_members) and its registration.
func newLedgerHarness(t *testing.T) *dbHarness {
	t.Helper()
	h := newDBHarness(t)
	body := gooseBody(dbLedgersMigration, "DROP TABLE db_ledgers;\nDROP TYPE ledger_kind;")
	path := filepath.Join("src", "database", "migrations", "00002_db_ledgers.sql")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	err := database.Register(h.svc, database.Registration[dbLedger, dbLedgerResource, dbLedgerRequest, string]{
		Channel: "ledgers",
		ToResource: func(l *dbLedger) *dbLedgerResource {
			return &dbLedgerResource{ID: l.ID, Kind: l.Kind, Status: l.Status, Meta: l.Meta}
		},
	})
	if err != nil {
		t.Fatalf("Register ledger: %v", err)
	}
	return h
}

func (h *dbHarness) startWithLedgers() (dbMemberService, dbLedgerService) {
	h.t.Helper()
	members := h.start()
	ledgers, err := database.Get[dbLedger, dbLedgerResource, dbLedgerRequest, string](h.svc)
	if err != nil {
		h.t.Fatalf("Get ledger: %v", err)
	}
	return members, ledgers
}

func byID(id string) pagination.StructuredFilter {
	return pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "id", Mode: pagination.ModeEqual, Value: id}}}
}

func byMember(id string) pagination.StructuredFilter {
	return pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "member_id", Mode: pagination.ModeEqual, Value: id}}}
}

var errOverLimit = errors.New("business rule: balance over limit")

// ---------------------------------------------------------------------------------------------------
// 1. One transaction across all three packages
// ---------------------------------------------------------------------------------------------------

// Sad: cqrs creates a member and a ledger entry, pagination reads and locks them inside the same
// transaction, the sql client updates the member, then a business rule fails. Ending the transaction
// with that error must remove every mutation: no member, no orphaned ledger row, nothing on the reader.
func TestDatabaseCross_SadBusinessFailureRollsBackAllPackages(t *testing.T) {
	h := newLedgerHarness(t)
	members, ledgers := h.startWithLedgers()

	tx, err := members.StartTx(bg)
	if err != nil {
		t.Fatal(err)
	}
	pipeline := func() error {
		// cqrs: parent then child.
		if _, err := members.CreateWithTx(bg, tx, dbMember{ID: "m1", Name: "Ann", Balance: 900}); err != nil {
			return err
		}
		if _, err := ledgers.CreateWithTx(bg, tx, dbLedger{ID: "l1", MemberID: "m1", Kind: "credit", Amount: 200}); err != nil {
			return err
		}
		// pagination: the same tx sees its own uncommitted rows (and locks them FOR UPDATE).
		m, err := members.GetByIDWithTx(bg, &tx, "m1")
		if err != nil {
			return fmt.Errorf("pagination inside tx: %w", err)
		}
		entries, err := ledgers.FindWithTx(bg, &tx, byMember("m1"))
		if err != nil || len(entries) != 1 {
			return fmt.Errorf("pagination inside tx found %d ledger rows: %v", len(entries), err)
		}
		// sql: a raw statement on the same transaction.
		if _, err := tx.NewUpdate().Model((*dbMember)(nil)).
			Set("balance = balance + ?", entries[0].Amount).Where("id = ?", m.ID).Exec(bg); err != nil {
			return err
		}
		var balance int64
		if err := tx.NewSelect().Model((*dbMember)(nil)).Column("balance").Where("id = ?", m.ID).Scan(bg, &balance); err != nil {
			return err
		}
		if balance > 1000 {
			return errOverLimit
		}
		return nil
	}

	if err := members.EndTx(bg, tx, pipeline()); !errors.Is(err, errOverLimit) {
		t.Fatalf("EndTx = %v; want the business failure", err)
	}
	for _, table := range []string{"db_members", "db_ledgers"} {
		if n := count(t, h.writer, `SELECT count(*) FROM `+table); n != 0 {
			t.Errorf("writer %s has %d rows after rollback; want 0", table, n)
		}
		if n := count(t, h.reader, `SELECT count(*) FROM `+table); n != 0 {
			t.Errorf("reader %s has %d rows after rollback; want 0", table, n)
		}
	}
}

// Sad: a database error halfway through (a ledger row for a member that does not exist) aborts the
// whole transaction, so the member created earlier in it is rolled back too.
func TestDatabaseCross_SadDatabaseErrorMidPipelineRollsBackEarlierWrites(t *testing.T) {
	h := newLedgerHarness(t)
	members, ledgers := h.startWithLedgers()

	tx, err := members.StartTx(bg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = members.CreateWithTx(bg, tx, dbMember{ID: "m1", Name: "Ann"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ledgers.CreateWithTx(bg, tx, dbLedger{ID: "l1", MemberID: "ghost", Kind: "credit", Amount: 1})
	if pgCode(err) != "23503" {
		t.Fatalf("ledger for a missing member = %v; want a foreign key violation (23503)", err)
	}
	// Postgres has aborted the transaction; later statements in it must fail rather than half-apply.
	if _, err := members.IncrementByIDWithTx(bg, tx, "m1", "balance", 1); pgCode(err) != "25P02" {
		t.Errorf("statement after the failure = %v; want 25P02 (transaction aborted)", err)
	}
	if err := members.EndTx(bg, tx, err); err == nil {
		t.Fatal("EndTx returned nil for a failed pipeline")
	}
	if n := count(t, h.writer, `SELECT count(*) FROM db_members`); n != 0 {
		t.Fatalf("writer has %d members; the earlier create must be rolled back", n)
	}
}

// ---------------------------------------------------------------------------------------------------
// 2. Sequential pipeline: each step reads the exact state the previous steps left
// ---------------------------------------------------------------------------------------------------

// Happy: cqrs writes (A), cqrs increments and adds a child row (B), the runner syncs, and pagination
// (C) reads exactly those values from the reader. A second round proves the reader is not stale.
func TestDatabaseCross_HappySequentialPipelineReadsLatestState(t *testing.T) {
	h := newLedgerHarness(t)
	members, ledgers := h.startWithLedgers()
	h.svc.Run(bg)
	memberEvents, ledgerEvents := h.broker.await(t, "members"), h.broker.await(t, "ledgers")

	// A
	m, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann", Balance: 100})
	if err != nil {
		t.Fatal(err)
	}
	dbPublish(t, memberEvents, "m-1", cqrs.ChangeTypeCreated, *m)
	awaitCount(t, h.reader, 1, `SELECT count(*) FROM db_members WHERE id = 'm1'`)

	// B
	l, err := ledgers.Create(bg, dbLedger{ID: "l1", MemberID: "m1", Kind: "credit", Amount: 50, Meta: map[string]any{"step": "B"}})
	if err != nil {
		t.Fatal(err)
	}
	if m, err = members.IncrementByID(bg, "m1", "balance", 50); err != nil {
		t.Fatal(err)
	}
	dbPublish(t, ledgerEvents, "l-1", cqrs.ChangeTypeCreated, *l)
	dbPublish(t, memberEvents, "m-2", cqrs.ChangeTypeUpdated, *m)
	awaitCount(t, h.reader, 150, `SELECT COALESCE(SUM(balance), 0) FROM db_members`)
	awaitCount(t, h.reader, 1, `SELECT count(*) FROM db_ledgers`)

	// C
	got, err := members.GetByID(bg, "m1")
	if err != nil || got.Balance != 150 {
		t.Fatalf("C read member = %+v, %v; want balance 150 from A+B", got, err)
	}
	entries, err := ledgers.Find(bg, byMember("m1"))
	if err != nil || len(entries) != 1 || entries[0].Amount != 50 || entries[0].Meta["step"] != "B" {
		t.Fatalf("C read ledger = %+v, %v; want the single entry B wrote", entries, err)
	}

	// Second round: the reader must reflect the newest write, not a cached earlier one.
	if m, err = members.IncrementByID(bg, "m1", "balance", 25); err != nil {
		t.Fatal(err)
	}
	dbPublish(t, memberEvents, "m-3", cqrs.ChangeTypeUpdated, *m)
	awaitCount(t, h.reader, 175, `SELECT balance FROM db_members WHERE id = 'm1'`)
	if got, err = members.GetByID(bg, "m1"); err != nil || got.Balance != 175 {
		t.Fatalf("second read = %+v, %v; want 175 (a stale read returns 150)", got, err)
	}
}

// Happy: inside one transaction every step reads the previous step's uncommitted writes, while a
// reader outside the transaction sees nothing until commit.
func TestDatabaseCross_HappyInTxPipelineReadsOwnWritesAndIsIsolated(t *testing.T) {
	h := newLedgerHarness(t)
	members, ledgers := h.startWithLedgers()

	tx, err := members.StartTx(bg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := members.CreateWithTx(bg, tx, dbMember{ID: "m1", Name: "Ann", Balance: 100}); err != nil {
		t.Fatal(err)
	}
	if _, err := members.IncrementByIDWithTx(bg, tx, "m1", "balance", 50); err != nil {
		t.Fatal(err)
	}
	if _, err := ledgers.CreateWithTx(bg, tx, dbLedger{ID: "l1", MemberID: "m1", Kind: "debit", Amount: 30}); err != nil {
		t.Fatal(err)
	}
	got, err := members.GetByIDWithTx(bg, &tx, "m1")
	if err != nil || got.Balance != 150 {
		t.Fatalf("read inside tx = %+v, %v; want 150", got, err)
	}
	if n, err := ledgers.CountWithTx(bg, &tx, byMember("m1")); err != nil || n != 1 {
		t.Fatalf("CountWithTx = %d, %v; want 1", n, err)
	}
	if n := count(t, h.writer, `SELECT count(*) FROM db_members`); n != 0 {
		t.Fatalf("another connection sees %d uncommitted members; want 0", n)
	}
	if err := members.EndTx(bg, tx, nil); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := count(t, h.writer, `SELECT balance FROM db_members WHERE id = 'm1'`); n != 150 {
		t.Fatalf("committed balance = %d; want 150", n)
	}
}

// ---------------------------------------------------------------------------------------------------
// 3. Concurrency: many goroutines on the same rows
// ---------------------------------------------------------------------------------------------------

// runParallel runs fn n times concurrently under a deadline and returns every error, so a deadlock
// shows up as a context timeout instead of a hung test.
func runParallel(t *testing.T, n int, fn func(ctx context.Context, i int) error) []error {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 60*time.Second)
	defer cancel()
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for i := range n {
		wg.Go(func() {
			if err := fn(ctx, i); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return errs
}

// Happy: 100 concurrent atomic increments on one row lose nothing.
func TestDatabaseCross_HappyConcurrentIncrementsLoseNoUpdates(t *testing.T) {
	h := newLedgerHarness(t)
	members, _ := h.startWithLedgers()
	if _, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann"}); err != nil {
		t.Fatal(err)
	}

	errs := runParallel(t, 100, func(ctx context.Context, _ int) error {
		_, err := members.IncrementByID(ctx, "m1", "balance", 1)
		return err
	})
	if len(errs) > 0 {
		t.Fatalf("%d increments failed, first: %v", len(errs), errs[0])
	}
	if got := count(t, h.writer, `SELECT balance FROM db_members WHERE id = 'm1'`); got != 100 {
		t.Fatalf("balance = %d after 100 increments; want 100", got)
	}
}

// Happy: 50 transactions each read-modify-write the same member (locked by pagination's FOR UPDATE)
// and insert a ledger row. No update is lost and the balance equals the ledger total.
func TestDatabaseCross_HappyLockedReadModifyWriteHasNoLostUpdates(t *testing.T) {
	h := newLedgerHarness(t)
	members, ledgers := h.startWithLedgers()
	if _, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann"}); err != nil {
		t.Fatal(err)
	}

	errs := runParallel(t, 50, func(ctx context.Context, i int) error {
		tx, err := members.StartTx(ctx)
		if err != nil {
			return err
		}
		return members.EndTx(ctx, tx, func() error {
			m, err := members.GetByIDWithTx(ctx, &tx, "m1") // SELECT ... FOR UPDATE
			if err != nil {
				return err
			}
			m.Balance += 2
			if _, err := members.UpdateByIDWithTx(ctx, tx, "m1", *m); err != nil {
				return err
			}
			_, err = ledgers.CreateWithTx(ctx, tx, dbLedger{ID: fmt.Sprintf("l%03d", i), MemberID: "m1", Kind: "credit", Amount: 2})
			return err
		}())
	})
	if len(errs) > 0 {
		t.Fatalf("%d transactions failed, first: %v", len(errs), errs[0])
	}
	balance := count(t, h.writer, `SELECT balance FROM db_members WHERE id = 'm1'`)
	total := count(t, h.writer, `SELECT COALESCE(SUM(amount), 0) FROM db_ledgers WHERE member_id = 'm1'`)
	if balance != 100 || total != 100 {
		t.Fatalf("balance = %d, ledger total = %d; want both 100 (a lost update makes balance smaller)", balance, total)
	}
}

// Happy: 60 transfers between two members in both directions. Locking both rows through pagination
// in a fixed order (id ASC, FOR UPDATE) means no deadlock, and money is conserved.
func TestDatabaseCross_HappyOppositeTransfersDoNotDeadlock(t *testing.T) {
	h := newLedgerHarness(t)
	members, _ := h.startWithLedgers()
	for _, id := range []string{"m1", "m2"} {
		if _, err := members.Create(bg, dbMember{ID: id, Name: id, Balance: 1000}); err != nil {
			t.Fatal(err)
		}
	}
	both := pagination.StructuredFilter{
		Filters:    []pagination.Filter{{Field: "id", Mode: pagination.ModeInside, Value: []string{"m1", "m2"}}},
		SortFields: []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}},
	}

	errs := runParallel(t, 60, func(ctx context.Context, i int) error {
		from, to := "m1", "m2"
		if i%2 == 1 {
			from, to = to, from
		}
		tx, err := members.StartTx(ctx)
		if err != nil {
			return err
		}
		return members.EndTx(ctx, tx, func() error {
			rows, err := members.FindWithTx(ctx, &tx, both) // locks m1 then m2, whatever the direction
			if err != nil {
				return err
			}
			if len(rows) != 2 {
				return fmt.Errorf("locked %d rows; want 2", len(rows))
			}
			if _, err := members.IncrementByIDWithTx(ctx, tx, from, "balance", -7); err != nil {
				return err
			}
			_, err = members.IncrementByIDWithTx(ctx, tx, to, "balance", 7)
			return err
		}())
	})
	for _, err := range errs {
		if pgCode(err) == "40P01" {
			t.Fatalf("deadlock detected: %v", err)
		}
	}
	if len(errs) > 0 {
		t.Fatalf("%d transfers failed, first: %v", len(errs), errs[0])
	}
	if total := count(t, h.writer, `SELECT SUM(balance) FROM db_members`); total != 2000 {
		t.Fatalf("total balance = %d; want 2000 (money created or destroyed)", total)
	}
	if m1 := count(t, h.writer, `SELECT balance FROM db_members WHERE id = 'm1'`); m1 != 1000 {
		t.Fatalf("m1 = %d; want 1000 after 30 transfers each way", m1)
	}
}

// Happy: 100 goroutines deliver 100 events (each twice, concurrently) to the runner while 20 more
// goroutines paginate the reader. Every member lands exactly once and no read fails.
func TestDatabaseCross_HappyConcurrentDeliveryAndReads(t *testing.T) {
	h := newLedgerHarness(t)
	members, _ := h.startWithLedgers()
	handler := h.run()

	errs := runParallel(t, 220, func(ctx context.Context, i int) error {
		if i >= 200 {
			_, err := members.Paginate(ctx, pagination.Pagination{PageSize: 10})
			return err
		}
		n := i % 100 // each event is sent by two goroutines
		value := fmt.Appendf(nil, `{"event_id":"e%03d","change_type":%d,"payload":{"id":"m%03d","name":"n","balance":%d}}`,
			n, cqrs.ChangeTypeCreated, n, n)
		return handler(fmt.Appendf(nil, "e%03d", n), value)
	})
	if len(errs) > 0 {
		t.Fatalf("%d calls failed, first: %v", len(errs), errs[0])
	}
	awaitCount(t, h.reader, 100, `SELECT count(*) FROM db_members`)
	awaitCount(t, h.reader, 100, `SELECT count(*) FROM processed_events`)
	if sum := count(t, h.reader, `SELECT SUM(balance) FROM db_members`); sum != 4950 {
		t.Fatalf("reader balance sum = %d; want 4950", sum)
	}
}

// ---------------------------------------------------------------------------------------------------
// 4. Schema boundaries: values crossing every layer unchanged
// ---------------------------------------------------------------------------------------------------

// Happy: an entity cqrs created on the writer (enum, nested JSONB, database defaults) is handed as-is to
// the sync stream, lands on the reader, is matched by pagination filters on the enum and the default,
// and comes out of the ToResource serializer with the same values.
func TestDatabaseCross_HappyEnumJSONBAndDefaultsSurviveAllLayers(t *testing.T) {
	h := newLedgerHarness(t)
	members, ledgers := h.startWithLedgers()
	h.svc.Run(bg)
	memberEvents, ledgerEvents := h.broker.await(t, "members"), h.broker.await(t, "ledgers")

	m, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann"})
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string]any{"source": "teller", "tags": []any{"a", "b"}, "rate": 12.5, "nested": map[string]any{"ok": true}}
	created, err := ledgers.Create(bg, dbLedger{ID: "l1", MemberID: "m1", Kind: "debit", Amount: 40, Meta: meta})
	if err != nil {
		t.Fatalf("Create ledger: %v", err)
	}
	if created.Status != "posted" || created.CreatedAt.IsZero() {
		t.Fatalf("database defaults not returned by Create: status=%q created_at=%v", created.Status, created.CreatedAt)
	}

	dbPublish(t, memberEvents, "m-1", cqrs.ChangeTypeCreated, *m)
	awaitCount(t, h.reader, 1, `SELECT count(*) FROM db_members`)
	dbPublish(t, ledgerEvents, "l-1", cqrs.ChangeTypeCreated, *created)
	awaitCount(t, h.reader, 1, `SELECT count(*) FROM db_ledgers`)

	filter := pagination.StructuredFilter{Filters: []pagination.Filter{
		{Field: "kind", Mode: pagination.ModeEqual, Value: "debit"},
		{Field: "status", Mode: pagination.ModeEqual, Value: "posted"},
	}}
	found, err := ledgers.FindFormat(bg, filter)
	if err != nil || len(found) != 1 {
		t.Fatalf("FindFormat on enum + default = %d rows, %v; want 1", len(found), err)
	}
	res := found[0]
	if res.Kind != "debit" || res.Status != "posted" || !reflect.DeepEqual(res.Meta, meta) {
		t.Fatalf("resource after writer -> stream -> reader -> serializer = %+v; want kind debit, status posted, meta %v", res, meta)
	}
	if got := count(t, h.reader, `SELECT count(*) FROM db_ledgers WHERE meta->'nested'->>'ok' = 'true' AND created_at = $1`, created.CreatedAt); got != 1 {
		t.Fatal("reader row lost the JSONB structure or the writer's created_at")
	}
}

// Sad: values that break a database rule are rejected with the precise Postgres error and leave
// nothing behind for pagination to find.
func TestDatabaseCross_SadConstraintViolationsAreRejectedAtTheBoundary(t *testing.T) {
	h := newLedgerHarness(t)
	members, ledgers := h.startWithLedgers()
	if _, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann"}); err != nil {
		t.Fatal(err)
	}

	for name, tc := range map[string]struct {
		row  dbLedger
		code string
	}{
		"unknown member (foreign key)": {dbLedger{ID: "l1", MemberID: "ghost", Kind: "credit", Amount: 1}, "23503"},
		"value outside the enum":       {dbLedger{ID: "l2", MemberID: "m1", Kind: "refund", Amount: 1}, "22P02"},
		"amount fails CHECK":           {dbLedger{ID: "l3", MemberID: "m1", Kind: "credit", Amount: 0}, "23514"},
	} {
		if _, err := ledgers.Create(bg, tc.row); pgCode(err) != tc.code {
			t.Errorf("%s: Create = %v; want SQLSTATE %s", name, err, tc.code)
		}
	}
	if n := count(t, h.writer, `SELECT count(*) FROM db_ledgers`); n != 0 {
		t.Fatalf("writer has %d ledger rows after only invalid writes; want 0", n)
	}
	if err := members.DeleteByID(bg, "m1"); err != nil {
		t.Fatalf("deleting a member with no ledger rows: %v", err)
	}
}

// Sad: runners for different models are independent, so a child can reach the reader before its
// parent. The reader's foreign key rejects that batch without recording the event as processed, so a
// redelivery after the parent arrives applies it. (In production the redelivery must come from the
// broker; see the batch-acknowledgement note in the database service review.)
func TestDatabaseCross_SadChildBeforeParentIsRejectedThenRecovers(t *testing.T) {
	h := newLedgerHarness(t)
	members, ledgers := h.startWithLedgers()
	h.svc.Run(bg)
	memberEvents, ledgerEvents := h.broker.await(t, "members"), h.broker.await(t, "ledgers")

	m, err := members.Create(bg, dbMember{ID: "m1", Name: "Ann"})
	if err != nil {
		t.Fatal(err)
	}
	l, err := ledgers.Create(bg, dbLedger{ID: "l1", MemberID: "m1", Kind: "credit", Amount: 5})
	if err != nil {
		t.Fatal(err)
	}

	dbPublish(t, ledgerEvents, "l-1", cqrs.ChangeTypeCreated, *l) // the child arrives first
	time.Sleep(300 * time.Millisecond)                            // several flush intervals
	if n := count(t, h.reader, `SELECT count(*) FROM db_ledgers`); n != 0 {
		t.Fatalf("reader accepted a ledger row whose member is missing (%d rows)", n)
	}
	if n := count(t, h.reader, `SELECT count(*) FROM processed_events WHERE event_id = 'l-1'`); n != 0 {
		t.Fatal("the rejected event was recorded as processed, so a redelivery would be skipped")
	}

	dbPublish(t, memberEvents, "m-1", cqrs.ChangeTypeCreated, *m)
	awaitCount(t, h.reader, 1, `SELECT count(*) FROM db_members`)
	dbPublish(t, ledgerEvents, "l-1", cqrs.ChangeTypeCreated, *l) // redelivery
	awaitCount(t, h.reader, 1, `SELECT count(*) FROM db_ledgers WHERE member_id = 'm1'`)
}
