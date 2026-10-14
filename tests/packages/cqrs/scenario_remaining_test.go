//go:build integration

package cqrs_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// ===========================================================================
// Extended network partition between the write and read infrastructure, and the
// bulk catch-up of the backlog that builds up meanwhile. Nothing is delivered
// for a long time while the write side keeps working, then everything arrives.
// ===========================================================================

func TestPartition_HugeBacklogCatchesUp(t *testing.T) {
	t.Parallel()
	r := newRig(t, 4)
	pm := r.w.startPump(t, r.f, 400)
	pm.down.Store(true)
	r.w.bulkInsert(1, 6000, "p")
	r.w.bulkUpdate(1, 6000, 2)
	r.w.bulkRemove(5000, 5500)
	time.Sleep(100 * time.Millisecond)
	pm.down.Store(false)
	r.waitInSyncFor(2 * time.Minute)
	eventuallyN(t, 30*time.Second, "the end of the log", pm.caughtUp)
}

func TestPartition_BacklogDeliveredInOneGiantBatch(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, func(c *cqrsSvc) { c.BatchSize = 5000 })
	r.w.bulkInsert(1, 4000, "g")
	r.w.bulkUpdate(1, 4000, 1)
	if err := r.f.deliver(longCtx(t), r.w.slice(0, 0, 8000)); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	if s := r.stats(); s.Applied != 4000 {
		t.Fatalf("a giant batch collapses to the final state of each key: applied %d, want 4000", s.Applied)
	}
}

func TestPartition_BacklogInTinyBatches(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.bulkInsert(1, 300, "t")
	r.w.bulkUpdate(1, 300, 1)
	if err := r.w.deliverNew(longCtx(t), r.f, 1); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	if s := r.stats(); s.Batches != 600 {
		t.Fatalf("batches = %d, want one per record", s.Batches)
	}
}

func TestPartition_WritesContinueDuringCatchUp(t *testing.T) {
	t.Parallel()
	r := newRig(t, 3)
	pm := r.w.startPump(t, r.f, 100)
	pm.down.Store(true)
	r.w.bulkInsert(1, 2000, "a")
	pm.down.Store(false)
	next := int64(5000)
	for range 6 {
		r.w.bulkInsert(next, 100, "live")
		next += 100
		r.w.bulkUpdate(1, 2000, 1)
		time.Sleep(20 * time.Millisecond)
	}
	r.waitInSyncFor(2 * time.Minute)
}

func TestPartition_RepeatedLongPartitions(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 150)
	next := int64(1)
	for round := range 5 {
		pm.down.Store(true)
		r.w.bulkInsert(next, 400, fmt.Sprintf("r%d-", round))
		r.w.bulkUpdate(1, next+399, 1)
		next += 400
		time.Sleep(60 * time.Millisecond)
		pm.down.Store(false)
		time.Sleep(60 * time.Millisecond)
	}
	r.waitInSyncFor(2 * time.Minute)
}

func TestPartition_BacklogWithEveryChangeKind(t *testing.T) {
	t.Parallel()
	r := newRig(t, 3)
	pm := r.w.startPump(t, r.f, 120)
	pm.down.Store(true)
	for _, sc := range scripts() {
		// Each script starts from an empty table, as it was written to; the events of
		// the earlier scripts stay in the log, so the backlog holds all of them.
		r.w.bulkRemoveAll()
		sc.run(r.w)
	}
	pm.down.Store(false)
	r.waitInSyncFor(2 * time.Minute)
}

func TestPartition_ReadSideAlsoDownDuringTheCatchUp(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 200)
	pm.down.Store(true)
	r.w.bulkInsert(1, 3000, "b")
	restore := readOutage(t, r.p)
	pm.down.Store(false)
	time.Sleep(300 * time.Millisecond)
	restore()
	r.waitInSyncFor(2 * time.Minute)
}

func TestPartition_CatchUpUsesFewTransactions(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, func(c *cqrsSvc) { c.BatchSize = 1000 })
	r.w.bulkInsert(1, 5000, "x")
	if err := r.w.deliverNew(longCtx(t), r.f, 1000); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	if s := r.stats(); s.Batches != 5 {
		t.Fatalf("5000 records in batches of 1000 took %d batches", s.Batches)
	}
}

func TestPartition_BacklogOfOnlyDeletes(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 150)
	r.w.bulkInsert(1, 1500, "d")
	r.waitInSync()
	pm.down.Store(true)
	r.w.bulkRemove(1, 1500)
	time.Sleep(50 * time.Millisecond)
	pm.down.Store(false)
	r.waitInSyncFor(2 * time.Minute)
	if n := r.p.count(r.p.read, "accounts"); n != 0 {
		t.Fatalf("rows left = %d", n)
	}
}

func TestPartition_BacklogOfOnlyUpdatesToOneRow(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	pm := r.w.startPump(t, r.f, 100)
	r.w.insert(1, "one", 0)
	r.waitInSync()
	pm.down.Store(true)
	for i := int64(1); i <= 600; i++ {
		r.w.bumpTo(1, i)
	}
	pm.down.Store(false)
	r.waitInSync()
	if a := r.p.readAccounts()[1]; a.Balance != 600 || a.Version != 601 {
		t.Fatalf("row = %+v", a)
	}
}

func TestPartition_TwoReplicasDrainTheBacklogTogether(t *testing.T) {
	t.Parallel()
	r := newRig(t, 4)
	f2, s2 := r.replica()
	r.w.bulkInsert(1, 4000, "b")
	r.w.bulkUpdate(1, 4000, 1)
	ctx := longCtx(t)
	errs := make(chan error, 2)
	for _, f := range []*fakeCDC{r.f, f2} {
		go func() { errs <- r.w.deliverNew(ctx, f, 250) }()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	r.p.assertInSync(t)
	if got := r.svc.Stats().Applied + s2.Stats().Applied; got != 8000 {
		t.Fatalf("the two replicas wrote %d rows between them, want 8000", got)
	}
}

func TestPartition_BacklogAcrossAConsumerRestart(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.bulkInsert(1, 3000, "b")
	ctx := longCtx(t)
	for part := range r.w.parts {
		if err := r.f.deliver(ctx, r.w.slice(part, 0, 300)); err != nil {
			t.Fatal(err)
		}
	}
	f2, _ := r.replica() // the consumer was replaced half way
	if err := r.w.deliverNew(ctx, f2, 300); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

// ===========================================================================
// Write-database failover or primary promotion mid-sync. The write database
// the application writes to fails over while changes are being replicated. The
// replica keeps working from the events it already has; after the promotion the
// application's writes resume and are replicated too. The read model must always
// equal what the (new) write primary holds.
// ===========================================================================

func TestWriteFailover_ReplicationContinuesFromTheLogDuringTheOutage(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 40)
	r.w.bulkInsert(1, 200, "a")
	r.w.bulkUpdate(1, 200, 1)
	restore := writeOutage(t, r.p)
	// The events already produced are still replicated although the write database is gone.
	r.waitInSyncAgainstLog(t, r.w)
	restore()
	settleWrite(t, r.p)
	r.waitInSync()
	_ = pm
}

func TestWriteFailover_WritesRetryAcrossThePromotion(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.startPump(t, r.f, 25)
	r.w.bulkInsert(1, 100, "a")
	r.waitInSync()
	setWriteReadOnly(t, r.p, true)
	killWriteBackends(t, r.p)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for id := int64(200); id < 230; id++ {
			r.w.insertEventually(id, "after", id) // the application keeps retrying until the new primary accepts it
		}
	}()
	time.Sleep(300 * time.Millisecond)
	setWriteReadOnly(t, r.p, false) // promotion
	killWriteBackends(t, r.p)
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("writes never resumed after the promotion")
	}
	r.waitInSync()
}

func TestWriteFailover_RepeatedFailovers(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.startPump(t, r.f, 30)
	next := int64(1)
	for range 5 {
		r.w.bulkInsert(next, 30, "f")
		next += 30
		restore := writeOutage(t, r.p)
		time.Sleep(100 * time.Millisecond)
		restore()
		settleWrite(t, r.p)
		r.w.bulkUpdate(1, next-1, 1)
	}
	r.waitInSync()
}

func TestWriteFailover_ReadReplicationUnaffectedByWriteReadOnly(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.startPump(t, r.f, 50)
	r.w.bulkInsert(1, 60, "a")
	setWriteReadOnly(t, r.p, true)
	r.waitInSync() // the read side does not depend on the write database accepting writes
	setWriteReadOnly(t, r.p, false)
	settleWrite(t, r.p)
	r.w.bulkUpdate(1, 60, 4)
	r.waitInSync()
}

func TestWriteFailover_FailoverAndReadOutageTogether(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.startPump(t, r.f, 40)
	r.w.bulkInsert(1, 100, "a")
	r.waitInSync()
	restoreW := writeOutage(t, r.p)
	restoreR := readOutage(t, r.p)
	time.Sleep(300 * time.Millisecond)
	restoreW()
	settleWrite(t, r.p)
	r.w.bulkUpdate(1, 100, 3)
	restoreR()
	r.waitInSync()
}

func TestWriteFailover_NewPrimaryHasFewerRowsThanTheOldOne(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 100, "a")
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	// Asynchronous replication lost the last writes: the promoted primary does not
	// have rows 91..100 although the events for them were already emitted. The
	// application (or an operator) then deletes what the new primary must not have.
	r.w.bulkRemove(91, 100) // the connector reads the new primary's log: deletes for what is gone
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

func TestWriteFailover_InFlightTransactionsAreLostNotHalfReplicated(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.startPump(t, r.f, 50)
	r.w.bulkInsert(1, 20, "a")
	r.waitInSync()
	tx, err := r.p.write.Client().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for id := 100; id < 120; id++ {
		if _, err := tx.Exec(`INSERT INTO accounts (id, owner) VALUES (?, 'inflight')`, id); err != nil {
			t.Fatal(err)
		}
	}
	killWriteBackends(t, r.p) // the primary dies with the transaction open: it never committed
	_ = tx.Rollback()
	settleWrite(t, r.p)
	r.w.bulkInsert(200, 10, "after")
	r.waitInSync()
	for id := range r.p.readAccounts() {
		if id >= 100 && id < 120 {
			t.Fatalf("row %d of a transaction that never committed reached the read model", id)
		}
	}
}

func TestWriteFailover_UnknownCommitOutcomeIsSafeToRetry(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.startPump(t, r.f, 50)
	for id := int64(1); id <= 30; id++ {
		r.w.insertEventually(id, "x", id)
		r.w.insertEventually(id, "x", id) // a retry of a write that may already have committed
	}
	r.waitInSync()
	if n := len(r.p.readAccounts()); n != 30 {
		t.Fatalf("rows = %d", n)
	}
}

func TestWriteFailover_ReplicasKeepApplyingWhileThePrimaryIsDown(t *testing.T) {
	t.Parallel()
	r := newRig(t, 3)
	f2, _ := r.replica()
	r.w.startPump(t, r.f, 40)
	r.w.startPump(t, f2, 40)
	r.w.bulkInsert(1, 300, "a")
	restore := writeOutage(t, r.p)
	r.waitInSyncAgainstLog(t, r.w)
	restore()
	settleWrite(t, r.p)
	r.w.bulkUpdate(1, 300, 2)
	r.waitInSync()
}

func TestWriteFailover_ChecksFailFastWhileTheWriteDatabaseIsGone(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-wf-run")
	svc := newService(t, p, f)
	restore := writeOutage(t, p)
	if err := svc.Run(longCtx(t)); !errors.Is(err, cqrs.ErrWriteDBUnreachable) {
		t.Fatalf("Run with the write database down = %v", err)
	}
	restore()
	settleWrite(t, p)
	start(t, svc)
}

func TestWriteFailover_StartingWithABacklogAfterAFailover(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	w := newWorld(t, p, 2)
	f := newFakeCDC("g-wf-backlog")
	svc := newService(t, p, f)
	w.bulkInsert(1, 500, "a")
	w.bulkUpdate(1, 500, 1)
	restore := writeOutage(t, p)
	restore()
	settleWrite(t, p)
	start(t, svc)
	pm := w.startPump(t, f, 100)
	eventuallyN(t, time.Minute, "the backlog", pm.caughtUp)
	p.assertInSync(t)
}

// waitInSyncAgainstLog waits for the read database to hold exactly what the log
// of events says, which is checkable while the write database is unreachable.
func (r *rig) waitInSyncAgainstLog(t testing.TB, w *world) {
	t.Helper()
	want := w.finalFromLog()
	deadline := time.Now().Add(60 * time.Second)
	for {
		got, err := accountsOf(r.p.read.Client())
		if err == nil {
			if d := diffAccounts(want, got, nil); d == "" {
				return
			} else if time.Now().After(deadline) {
				t.Fatalf("the read model never matched the event log: %s", d)
			}
		} else if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

// ===========================================================================
// Schema version mismatch during long-outage reconciliation. The read model was
// migrated while the service was down (or the service was deployed before the
// migration). The backlog of old events is then replayed against a newer, or an
// older, read schema.
// ===========================================================================

func TestSchemaMismatch_BacklogReplaysAgainstANewerReadSchema(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.bulkInsert(1, 100, "old")
	r.w.bulkUpdate(1, 100, 1)
	// The read model gained nullable columns during the outage that the service has
	// never heard of: the old events simply leave them NULL.
	r.p.exec(r.p.read, `ALTER TABLE accounts ADD COLUMN region text, ADD COLUMN score integer DEFAULT 7`)
	if err := r.w.deliverNew(longCtx(t), r.f, 40); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	var scores int
	if err := r.p.read.Client().NewRaw(`SELECT count(*) FROM accounts WHERE score = 7 AND region IS NULL`).Scan(context.Background(), &scores); err != nil || scores != 100 {
		t.Fatalf("new columns: %d rows with the database default, err=%v", scores, err)
	}
}

func TestSchemaMismatch_NewRequiredColumnStallsUntilItHasADefault(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.bulkInsert(1, 30, "a")
	r.p.exec(r.p.read, `ALTER TABLE accounts ADD COLUMN tenant text NOT NULL DEFAULT 'x'`)
	r.p.exec(r.p.read, `ALTER TABLE accounts ALTER COLUMN tenant DROP DEFAULT`)
	pm := r.w.startPump(t, r.f, 50)
	time.Sleep(600 * time.Millisecond)
	if pm.caughtUp() || len(r.f.deadLettered()) != 0 {
		t.Fatal("a NOT NULL column the model cannot fill is a deployment problem: stall, never dead-letter")
	}
	r.p.exec(r.p.read, `ALTER TABLE accounts ALTER COLUMN tenant SET DEFAULT 'filled'`)
	r.waitInSync()
}

func TestSchemaMismatch_ReadSchemaBehindStallsThenCatchesUp(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.bulkInsert(1, 60, "a")
	r.p.exec(r.p.read, `ALTER TABLE accounts DROP COLUMN note`)
	pm := r.w.startPump(t, r.f, 30)
	time.Sleep(600 * time.Millisecond)
	if pm.caughtUp() || len(r.f.deadLettered()) != 0 {
		t.Fatal("a missing column must stall the stream, not skip or park it")
	}
	r.p.exec(r.p.read, `ALTER TABLE accounts ADD COLUMN note text`)
	r.waitInSync()
}

func TestSchemaMismatch_NewServiceOldEventsOverALongOutage(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	w := newWorld(t, p, 2)
	w.bulkInsert(1, 80, "old")
	w.bulkUpdate(1, 80, 1)
	p.exec(p.read, `ALTER TABLE accounts ADD COLUMN nickname text NOT NULL DEFAULT 'db-default', ADD COLUMN tier bigint`)
	f := newFakeCDC("g-sm-new")
	svc := newTyped[Newer](t, p, f, "version")
	start(t, svc)
	if err := w.deliverNew(longCtx(t), f, 30); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := p.read.Client().NewRaw(`SELECT count(*) FROM accounts WHERE nickname = '' AND tier IS NULL`).Scan(context.Background(), &n); err != nil || n != 80 {
		t.Fatalf("columns the old events lack must be written as zero values: n=%d err=%v", n, err)
	}
}

func TestSchemaMismatch_ColumnAddedToTheWriteSideMidReplay(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 40, "a")
	if err := r.w.deliverNew(ctx, r.f, 20); err != nil {
		t.Fatal(err)
	}
	r.w.exec(`ALTER TABLE accounts ADD COLUMN extra text DEFAULT 'e'`)
	r.w.bulkUpdate(1, 40, 1)
	r.w.insert(100, "with-extra", 1)
	if err := r.w.deliverNew(ctx, r.f, 20); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSyncColumns(t) // the columns the model knows
}

func TestSchemaMismatch_ColumnDroppedFromTheWriteSideMidReplay(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.exec(`ALTER TABLE accounts ADD COLUMN gone text DEFAULT 'g'`)
	r.w.bulkInsert(1, 40, "a")
	if err := r.w.deliverNew(ctx, r.f, 20); err != nil {
		t.Fatal(err)
	}
	r.w.exec(`ALTER TABLE accounts DROP COLUMN gone`)
	r.w.bulkUpdate(1, 40, 1)
	if err := r.w.deliverNew(ctx, r.f, 20); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSyncColumns(t)
}

func TestSchemaMismatch_RunNamesEveryMissingColumn(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	p.exec(p.read, `ALTER TABLE accounts DROP COLUMN note, DROP COLUMN active`)
	svc := newService(t, p, newFakeCDC("g-sm-run"))
	err := svc.Run(longCtx(t))
	if err == nil || !strings.Contains(err.Error(), "note") || !strings.Contains(err.Error(), "active") {
		t.Fatalf("Run must say which columns are missing: %v", err)
	}
}

func TestSchemaMismatch_EventsWithUnknownAndMissingColumnsMixed(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	batch := []cdc.Record{
		event(0, 0, cdc.OpCreate, 1, "null", `{"id":1,"owner":"a","balance":1,"version":1,"future_col":"x"}`),
		event(0, 1, cdc.OpCreate, 2, "null", `{"id":2,"owner":"b"}`),
		event(0, 2, cdc.OpUpdate, 1, "null", `{"id":1,"owner":"a2","balance":5,"version":2,"another_future":{"k":[1,2]}}`),
	}
	if err := r.f.deliver(longCtx(t), batch); err != nil {
		t.Fatal(err)
	}
	got := r.p.readAccounts()
	if got[1].Owner != "a2" || got[1].Balance != 5 || got[2].Owner != "b" || got[2].Balance != 0 {
		t.Fatalf("rows = %+v", got)
	}
}

func TestSchemaMismatch_TypeWidenedInTheReadModel(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.p.exec(r.p.read, `ALTER TABLE accounts ALTER COLUMN owner TYPE varchar(5000)`)
	r.w.bulkInsert(1, 20, "w")
	r.w.mutate(1, 20, "owner = ?", strings.Repeat("y", 4000))
	if err := r.w.deliverNew(longCtx(t), r.f, 10); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

func TestSchemaMismatch_TypeNarrowedInTheReadModelParksOnlyTheMisfits(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.p.exec(r.p.read, `ALTER TABLE accounts ALTER COLUMN owner TYPE varchar(10)`)
	var bad []int64
	for id := int64(1); id <= 30; id++ {
		owner := "short"
		if id%10 == 0 {
			owner = strings.Repeat("z", 50)
			bad = append(bad, id)
		}
		r.w.insert(id, owner, id)
	}
	if err := r.w.deliverNew(longCtx(t), r.f, 100); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t, bad...)
	if got := len(r.f.deadLettered()); got != len(bad) {
		t.Fatalf("parked %d, want %d (a value that cannot fit is poison for this schema)", got, len(bad))
	}
}

func TestSchemaMismatch_IndexAddedToTheReadModelDuringTheOutage(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.bulkInsert(1, 200, "i")
	r.w.bulkUpdate(1, 200, 1)
	r.p.exec(r.p.read, `CREATE INDEX accounts_owner_idx ON accounts (owner)`)
	r.p.exec(r.p.read, `CREATE INDEX accounts_balance_idx ON accounts (balance DESC, id)`)
	if err := r.w.deliverNew(longCtx(t), r.f, 50); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

func TestSchemaMismatch_DefaultChangedInTheReadModel(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.p.exec(r.p.read, `ALTER TABLE accounts ALTER COLUMN owner SET DEFAULT 'changed-default', ALTER COLUMN balance SET DEFAULT 99`)
	r.w.insert(1, "", 0) // zero values must be copied as zero, never replaced by a default
	if err := r.w.deliverNew(longCtx(t), r.f, 10); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

// assertInSyncColumns compares only the columns the model maps (the write side
// may have gained or lost others), which assertInSync already does by reading
// through the model; it exists to say so in the tests that rely on it.
func (p *pair) assertInSyncColumns(t testing.TB, except ...int64) { p.assertInSync(t, except...) }
