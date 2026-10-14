//go:build integration

package cqrs_test

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// A change the read database rejects for good is parked. The service then repairs
// the row by itself as soon as the read database accepts it: nothing is called.

// fastRepair makes the service look at parked rows every 100ms instead of every 30s.
func fastRepair(c *cqrsSvc) { c.RepairInterval = 100 * time.Millisecond }

// strictRead makes the read database stricter than the write one: a balance of 13
// is refused for good there, unless the row is brand new (version 1, so the plain
// inserts of the helpers, which set balance = id, never trip it). Every update
// that sets 13 is poison.
func strictRead(p *pair) {
	p.exec(p.read, `ALTER TABLE accounts DROP CONSTRAINT IF EXISTS balance_ok`)
	p.exec(p.read, `ALTER TABLE accounts ADD CONSTRAINT balance_ok CHECK (balance <> 13 OR version <= 1)`)
}

func relaxRead(p *pair) { p.exec(p.read, `ALTER TABLE accounts DROP CONSTRAINT IF EXISTS balance_ok`) }

// The scenario of the limit: parked, stale, then fixed without anyone calling anything.
func TestRepair_ParkedRowHealsByItselfOnceTheCauseIsFixed(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2, fastRepair)
	ctx := longCtx(t)
	strictRead(r.p)
	r.w.bulkInsert(1, 40, "a")
	r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id IN (5, 6, 7)`)
	r.w.emitRange(cdc.OpUpdate, 5, 7)
	if err := r.w.deliverNew(ctx, r.f, 20); err != nil {
		t.Fatal(err)
	}
	if got := r.stats().DeadLettered; got < 3 {
		t.Fatalf("expected parked changes, got %d", got)
	}
	// While the constraint stands the rows stay stale, and the service keeps trying.
	time.Sleep(500 * time.Millisecond)
	if d, _ := r.p.syncDiff(); d == "" {
		t.Fatal("the rows were expected to be stale while the constraint stands")
	}
	if got := r.svc.Stats().Awaiting; got != 3 {
		t.Fatalf("Awaiting = %d, want the 3 parked rows", got)
	}
	// The cause is fixed. That is all the operator does.
	relaxRead(r.p)
	r.waitInSync()
	eventually(t, "the parked rows to be forgotten", func() bool { return r.svc.Stats().Awaiting == 0 })
	if got := r.svc.Stats().Repaired; got != 3 {
		t.Fatalf("Repaired = %d, want 3", got)
	}
}

// A row parked many times is one row to repair.
func TestRepair_ARowParkedManyTimesIsRepairedOnce(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, fastRepair)
	ctx := longCtx(t)
	strictRead(r.p)
	r.w.bulkInsert(1, 10, "a")
	for range 4 {
		r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id = 4`)
		r.w.emitRange(cdc.OpUpdate, 4, 4)
	}
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	if got := r.stats().DeadLettered; got != 4 {
		t.Fatalf("DeadLettered = %d, want 4", got)
	}
	if got := r.svc.Stats().Awaiting; got != 1 {
		t.Fatalf("Awaiting = %d, want 1 row", got)
	}
	relaxRead(r.p)
	r.waitInSync()
	eventually(t, "the row to be forgotten", func() bool { return r.svc.Stats().Awaiting == 0 })
	if got := r.svc.Stats().Repaired; got != 1 {
		t.Fatalf("Repaired = %d, want 1", got)
	}
}

// A parked row the stream has since changed again ends at its latest state, not at
// what the parked event said: old events are never replayed.
func TestRepair_UsesTheCurrentRowNotTheParkedEvent(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, fastRepair)
	ctx := longCtx(t)
	strictRead(r.p)
	r.w.bulkInsert(1, 5, "a")
	r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id = 3`)
	r.w.emitRange(cdc.OpUpdate, 3, 3) // parked
	r.w.set(3, "latest", 77)          // the row moves on and applies fine
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	relaxRead(r.p)
	eventually(t, "the row to be forgotten", func() bool { return r.svc.Stats().Awaiting == 0 })
	if got := r.p.readAccounts()[3]; got.Owner != "latest" || got.Balance != 77 {
		t.Fatalf("the repair replayed the old parked event: %+v", got)
	}
	if got := r.svc.Stats().Repaired; got != 0 {
		t.Fatalf("a row that already agrees must not be rewritten, Repaired = %d", got)
	}
}

// A row deleted and created again since it was parked is not brought back wrong.
func TestRepair_DeletedAndRecreatedRowIsNotResurrectedWrong(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, fastRepair)
	ctx := longCtx(t)
	strictRead(r.p)
	r.w.bulkInsert(1, 4, "a")
	r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id = 2`)
	r.w.emitRange(cdc.OpUpdate, 2, 2) // parked
	r.w.remove(2)
	r.w.insert(2, "reborn", 5)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	relaxRead(r.p)
	r.waitInSync()
	if got := r.p.readAccounts()[2]; got.Owner != "reborn" {
		t.Fatalf("row 2 = %+v", got)
	}
}

// A row the write database no longer has is removed from the read database.
func TestRepair_RowDeletedInTheWriteDatabaseDisappearsToo(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, fastRepair)
	ctx := longCtx(t)
	strictRead(r.p)
	r.w.bulkInsert(1, 6, "a")
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	// Row 3 goes to a value the read db refuses and is then deleted for good, but the
	// delete is lost on its way (the connector never emitted it).
	r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id = 3`)
	r.w.emitRange(cdc.OpUpdate, 3, 3)
	r.w.exec(`DELETE FROM accounts WHERE id = 3`)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	if got := r.p.readAccounts()[3]; got.ID != 3 {
		t.Fatalf("row 3 should still be there with its old state while parked: %+v", got)
	}
	// With the constraint still standing the repair deletes it anyway: a delete is
	// never refused by a CHECK.
	r.waitInSync()
}

// A row that is still refused after the cause is "fixed" stays awaited; the rest of
// its chunk is repaired.
func TestRepair_StillRejectedRowDoesNotBlockTheOthers(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, fastRepair)
	ctx := longCtx(t)
	strictRead(r.p)
	r.w.bulkInsert(1, 30, "a")
	r.w.exec(`UPDATE accounts SET balance = 1000 WHERE id = 13`) // row 13 would be poison by id
	r.w.emitRange(cdc.OpUpdate, 13, 13)
	r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id IN (8, 9, 20)`)
	r.w.emitRange(cdc.OpUpdate, 8, 9)
	r.w.emitRange(cdc.OpUpdate, 20, 20)
	if err := r.w.deliverNew(ctx, r.f, 100); err != nil {
		t.Fatal(err)
	}
	if got := r.svc.Stats().Awaiting; got != 3 {
		t.Fatalf("Awaiting = %d, want 3", got)
	}
	// Row 20 gets a value that is accepted; 8 and 9 stay poison.
	r.w.exec(`UPDATE accounts SET balance = 5, version = version + 1 WHERE id = 20`)
	eventually(t, "row 20 to be repaired and forgotten", func() bool { return r.svc.Stats().Awaiting == 2 })
	if got := r.p.readAccounts()[20]; got.Balance != 5 {
		t.Fatalf("row 20 = %+v", got)
	}
	// Now the cause is fixed for the rest too.
	relaxRead(r.p)
	r.waitInSync()
	eventually(t, "everything to be forgotten", func() bool { return r.svc.Stats().Awaiting == 0 })
}

// Rows parked before the service started (or by another replica) are found on the
// dead-letter topic, so nothing depends on this process having been there.
func TestRepair_FindsRowsParkedBeforeItStarted(t *testing.T) {
	t.Parallel()
	// The first life of the service parks two rows and repairs nothing (the loop is
	// off), then the process goes away.
	r := newRig(t, 1, func(c *cqrsSvc) { c.RepairInterval = -1 })
	ctx := longCtx(t)
	strictRead(r.p)
	r.w.bulkInsert(1, 10, "a")
	r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id IN (2, 3)`)
	r.w.emitRange(cdc.OpUpdate, 2, 3)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	parked := r.f.deadLettered()
	if len(parked) != 2 {
		t.Fatalf("expected 2 parked records, got %d", len(parked))
	}
	if err := r.svc.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	// A new process: its Kafka already holds those records on the dead-letter topic.
	f2 := newFakeCDC(r.group)
	for _, rec := range parked {
		if err := f2.DeadLetter(ctx, rec, fmt.Errorf("parked earlier")); err != nil {
			t.Fatal(err)
		}
	}
	svc2 := newService(t, r.p, f2, fastRepair)
	start(t, svc2)
	relaxRead(r.p)
	r.waitInSync()
	eventually(t, "the rows to be forgotten", func() bool { return svc2.Stats().Awaiting == 0 })
	if got := svc2.Stats().Repaired; got != 2 {
		t.Fatalf("Repaired = %d, want 2", got)
	}
}

// A dead-letter topic that cannot be read does not stop replication or the repair
// of rows parked from now on.
func TestRepair_UnreadableDeadLetterTopicDoesNotBreakAnything(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, fastRepair)
	ctx := longCtx(t)
	r.f.mu.Lock()
	r.f.readDeadErr = fmt.Errorf("kafka is down")
	r.f.mu.Unlock()
	strictRead(r.p)
	r.w.bulkInsert(1, 10, "a")
	r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id = 5`)
	r.w.emitRange(cdc.OpUpdate, 5, 5)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	relaxRead(r.p)
	r.f.mu.Lock()
	r.f.readDeadErr = nil
	r.f.mu.Unlock()
	r.waitInSync()
}

// Records whose key cannot be read have no row to repair and are ignored.
func TestRepair_UnreadableKeysAreIgnored(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, fastRepair)
	ctx := longCtx(t)
	if err := r.f.DeadLetter(ctx, cdc.Record{Topic: testTopic, Key: []byte("not json"), Value: []byte("x")}, fmt.Errorf("bad")); err != nil {
		t.Fatal(err)
	}
	if err := r.f.DeadLetter(ctx, cdc.Record{Topic: testTopic, Key: []byte(`{"id":0}`), Value: []byte("x")}, fmt.Errorf("bad")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond)
	if got := r.svc.Stats().Awaiting; got != 0 {
		t.Fatalf("Awaiting = %d: keys that name no row cannot be repaired", got)
	}
}

// RepairInterval < 0 turns the loop off: parked rows stay as they are.
func TestRepair_CanBeSwitchedOff(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, func(c *cqrsSvc) { c.RepairInterval = -1 })
	ctx := longCtx(t)
	strictRead(r.p)
	r.w.bulkInsert(1, 6, "a")
	r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id = 2`)
	r.w.emitRange(cdc.OpUpdate, 2, 2)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	relaxRead(r.p)
	time.Sleep(700 * time.Millisecond)
	if d, _ := r.p.syncDiff(); d == "" {
		t.Fatal("the repair ran although it was switched off")
	}
	if got := r.svc.Stats().Awaiting; got != 0 {
		t.Fatalf("Awaiting = %d with the repair off", got)
	}
}

// Stop ends the loop and nothing is left running; a service can start again.
func TestRepair_StopEndsTheLoopAndRestartResumes(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, fastRepair)
	ctx := longCtx(t)
	strictRead(r.p)
	r.w.bulkInsert(1, 6, "a")
	r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id = 2`)
	r.w.emitRange(cdc.OpUpdate, 2, 2)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	if err := r.svc.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	relaxRead(r.p)
	time.Sleep(600 * time.Millisecond)
	if d, _ := r.p.syncDiff(); d == "" {
		t.Fatal("a stopped service repaired a row")
	}
	if err := r.svc.Run(ctx); err != nil {
		t.Fatal(err)
	}
	// Row 2 was parked in the fake's dead-letter list, which a restart reads again.
	r.waitInSync()
}

// Repair running next to live replication: the databases end equal however the two
// interleave.
func TestRepair_WhileReplicationRuns(t *testing.T) {
	t.Parallel()
	r := newRig(t, 3, fastRepair)
	pm := r.w.startPump(t, r.f, 25)
	strictRead(r.p)
	rng := rand.New(rand.NewPCG(5, 6))
	for range 8 {
		randomHistory(r.w, rng, 15)
		id := 1 + rng.Int64N(60)
		r.w.exec(`UPDATE accounts SET balance = 13, version = version + 1 WHERE id = ?`, id)
		r.w.emitRange(cdc.OpUpdate, id, id) // a row that does not exist emits nothing
		time.Sleep(10 * time.Millisecond)
	}
	for !pm.caughtUp() {
		time.Sleep(10 * time.Millisecond)
	}
	relaxRead(r.p)
	r.waitInSync()
}

// Whatever mix of poison and healthy changes arrives, once the cause is fixed the
// databases always end equal, with nothing left awaited.
func TestRepair_RandomPoisonAlwaysHealsOnceFixed(t *testing.T) {
	seeds := fuzzSeeds(40)
	labTest(t, 2, []func(*cqrsSvc){fastRepair}, func(l *lab) {
		for _, seed := range seeds {
			l.run(fmt.Sprintf("seed=%d", seed), func(t *testing.T, r *rig) {
				rng := rand.New(rand.NewPCG(seed, 99))
				ctx := longCtx(t)
				strictRead(r.p)
				for range 30 + rng.IntN(40) {
					id := int64(1 + rng.IntN(30))
					bal := int64(rng.IntN(100))
					if rng.IntN(6) == 0 {
						bal = 13
					}
					if _, ok := rowExists(r, id); ok {
						r.w.set(id, "x", bal)
					} else {
						r.w.insert(id, "x", bal)
					}
				}
				if err := r.w.deliverNew(ctx, r.f, 1+rng.IntN(25)); err != nil {
					t.Fatal(err)
				}
				relaxRead(r.p)
				r.waitInSync()
				eventually(t, "nothing left awaited", func() bool { return r.svc.Stats().Awaiting == 0 })
			})
		}
	})
}

// With OrderByLSN the old delete tombstones are pruned by the service itself, and
// only the old ones.
func TestRepair_PrunesOldTombstonesByItself(t *testing.T) {
	t.Parallel()
	// Pruning runs every retention/10 (at least every RepairInterval): 10s gives 1s.
	// The 3 young tombstones stay for 10s, far longer than the test needs.
	r := newRig(t, 1, withLSN, fastRepair, func(c *cqrsSvc) { c.RowStateRetention = 10 * time.Second })
	ctx := longCtx(t)
	r.w.bulkInsert(1, 10, "a")
	r.w.bulkRemove(1, 6) // 6 tombstones
	if err := r.w.deliverNew(ctx, r.f, 100); err != nil {
		t.Fatal(err)
	}
	count := func(where string) int {
		var n int
		if err := r.p.read.Client().NewRaw(`SELECT count(*) FROM cqrs_row_state WHERE `+where).Scan(ctx, &n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if count("deleted") != 6 || count("NOT deleted") != 4 {
		t.Fatalf("states: %d tombstones, %d live", count("deleted"), count("NOT deleted"))
	}
	r.p.exec(r.p.read, `UPDATE cqrs_row_state SET updated_at = now() - interval '2 hours' WHERE deleted AND row_key IN ('1','2','3')`)
	eventually(t, "the 3 old tombstones to be pruned", func() bool { return count("deleted") <= 3 })
	if count("deleted") != 3 {
		t.Fatalf("%d tombstones left, want only the 3 young ones", count("deleted"))
	}
	if count("NOT deleted") != 4 {
		t.Fatalf("live rows' states were pruned: %d left", count("NOT deleted"))
	}
}
