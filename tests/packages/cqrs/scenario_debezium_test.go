//go:build integration

package cqrs_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// ===========================================================================
// Debezium connector crash and restart mid-stream. A restarted connector may
// emit changes again (it resumes from its last flushed position). The read side
// must never apply a change twice or go backwards: the checkpoint skips what a
// redelivery of the same offsets repeats, and the version guard turns away what
// a re-emission at new offsets would undo.
//
// The first group of tests uses a model of the connector's behaviour so every
// timing is exact; the second group crashes a real connector.
// ===========================================================================

func TestConnectorRestart_ReEmittedChangesDoNotCorrupt(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 40)
	r.w.bulkInsert(1, 100, "a")
	r.w.bulkUpdate(1, 100, 5)
	r.waitInSync()
	r.w.replayAll() // the restarted connector repeats everything since its last flush
	r.w.bulkUpdate(1, 100, 1)
	r.waitInSync()
	if !pm.caughtUp() {
		t.Fatal("the replayed log must be consumed too")
	}
	for id, a := range r.p.readAccounts() {
		if a.Version != 3 || a.Balance != id+6 {
			t.Fatalf("row %d = %+v: a re-emitted change moved the row", id, a)
		}
	}
}

func TestConnectorRestart_ReEmissionAfterNewerChangeIsTurnedAway(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.startPump(t, r.f, 50)
	r.w.bulkInsert(1, 30, "a")
	r.w.bulkUpdate(1, 30, 10) // version 2
	r.w.bulkUpdate(1, 30, 10) // version 3
	r.waitInSync()
	stale0 := r.svc.Stats().Stale
	// The connector comes back and re-emits the first two generations of changes.
	r.w.duplicate(0, 0, 60)
	eventuallyN(t, 30*time.Second, "the re-emission to be consumed", func() bool { return r.f.committedOffset(0) == r.w.size(0) })
	r.waitInSync()
	if got := r.svc.Stats().Stale - stale0; got == 0 {
		t.Fatal("older versions re-emitted after newer ones must be turned away by the version guard")
	}
}

func TestConnectorRestart_SameOffsetsRedeliveredAreSkippedByCheckpoint(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 80, "a")
	r.w.bulkUpdate(1, 80, 3)
	if err := r.w.deliverNew(ctx, r.f, 40); err != nil {
		t.Fatal(err)
	}
	before := r.svc.Stats()
	if err := r.w.deliverRange(ctx, r.f, 0, 0, r.w.size(0), 40); err != nil {
		t.Fatal(err)
	}
	after := r.svc.Stats()
	if after.Applied != before.Applied {
		t.Fatalf("a redelivery applied %d rows again", after.Applied-before.Applied)
	}
	if got := after.Duplicates - before.Duplicates; got != r.w.size(0) {
		t.Fatalf("duplicates = %d, want %d", got, r.w.size(0))
	}
	r.p.assertInSync(t)
}

func TestConnectorRestart_RestartMidStreamThenMoreWrites(t *testing.T) {
	t.Parallel()
	r := newRig(t, 3)
	r.w.startPump(t, r.f, 25)
	next := int64(1)
	for range 5 {
		r.w.bulkInsert(next, 40, "g")
		next += 40
		r.w.duplicate(0, 0, min(r.w.size(0), 30)) // a restart repeats the tail it had not flushed
		r.w.duplicate(1, 0, min(r.w.size(1), 30))
		r.w.bulkUpdate(1, next-1, 1)
	}
	r.waitInSync()
}

func TestConnectorRestart_DeleteReEmittedAfterRecreate(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.startPump(t, r.f, 20)
	r.w.insert(1, "first", 1)
	removed := r.w.size(0)
	r.w.remove(1)
	r.w.insert(1, "second", 2)
	r.w.bumpTo(1, 5)
	r.waitInSync()
	// The connector re-emits the old delete (and the create before it).
	r.w.duplicate(0, 0, removed+2)
	eventuallyN(t, 30*time.Second, "the re-emitted log", func() bool { return r.f.committedOffset(0) == r.w.size(0) })
	// A delete of an older generation must not take away the row that exists now.
	r.p.assertInSync(t)
	if a, ok := r.p.readAccounts()[1]; !ok || a.Owner != "second" {
		t.Fatalf("the recreated row was removed by a replayed delete: %+v ok=%v", a, ok)
	}
}

func TestConnectorRestart_EveryPrefixOfTheLogConverges(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 40, "a")
	r.w.bulkUpdate(1, 40, 2)
	r.w.bulkRemove(30, 40)
	r.w.bulkInsert(30, 5, "b")
	r.w.bulkUpdate(1, 20, 4)
	// Whatever point a crash happens at, redelivering from any earlier offset must
	// lead to the same end state.
	size := r.w.size(0)
	for _, from := range []int64{0, 7, size / 3, size / 2, size - 5, 0} {
		if err := r.w.deliverRange(ctx, r.f, 0, from, size, 17); err != nil {
			t.Fatal(err)
		}
		r.p.assertInSync(t)
	}
}

func TestConnectorRestart_ConcurrentOldAndNewConnectorOutput(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.startPump(t, r.f, 30)
	r.w.bulkInsert(1, 120, "a")
	r.w.bulkUpdate(1, 120, 1)
	r.w.duplicate(0, 0, r.w.size(0)/2) // the old task's last output, then the new task's
	r.w.duplicate(1, 0, r.w.size(1)/2)
	r.w.bulkUpdate(1, 120, 1)
	r.waitInSync()
	for id, a := range r.p.readAccounts() {
		if a.Version != 3 || a.Balance != id+2 {
			t.Fatalf("row %d = %+v", id, a)
		}
	}
}

func TestConnectorRestart_HooksNotRepeatedForReEmission(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	r := newRig(t, 1, withHooks(rec))
	ctx := longCtx(t)
	r.w.bulkInsert(1, 30, "a")
	if err := r.w.deliverNew(ctx, r.f, 30); err != nil {
		t.Fatal(err)
	}
	eventuallyN(t, 15*time.Second, "30 hooks", func() bool { return rec.count() == 30 })
	if err := r.w.deliverRange(ctx, r.f, 0, 0, r.w.size(0), 30); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if rec.count() != 30 {
		t.Fatalf("hooks fired %d times; a redelivery must not notify again", rec.count())
	}
}

// ---- the real connector ----------------------------------------------------

func TestConnectorRestart_RealConnectorCrashMidStream(t *testing.T) {
	t.Parallel()
	x := newE2E(t)
	x.insertRange(1, 300, "pre")
	x.sync()
	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; !stop.Load() && i < 200; i++ {
			x.bump(1, 300, 1)
			time.Sleep(15 * time.Millisecond)
		}
	}()
	time.Sleep(300 * time.Millisecond)
	x.s.crashConnector(t, x.prefix) // dies and comes back while writes keep arriving
	time.Sleep(400 * time.Millisecond)
	x.s.crashConnector(t, x.prefix)
	stop.Store(true)
	<-done
	x.sync()
}

func TestConnectorRestart_RealConnectorRestartedManyTimes(t *testing.T) {
	t.Parallel()
	x := newE2E(t)
	next := int64(1)
	for range 4 {
		x.insertRange(next, 100, "r")
		next += 100
		x.s.crashConnector(t, x.prefix)
		x.bump(1, next-1, 1)
	}
	x.sync()
	for id, a := range x.p.readAccounts() {
		if a.Balance != id+4 || a.Version != 5 {
			// Rows created in later rounds have fewer bumps; only compare what is exact.
			if id <= 100 && (a.Balance != id+4 || a.Version != 5) {
				t.Fatalf("row %d = %+v", id, a)
			}
		}
	}
}

func TestConnectorRestart_RealConnectorWritesWhileItIsGone(t *testing.T) {
	t.Parallel()
	x := newE2E(t)
	x.insertRange(1, 50, "a")
	x.sync()
	x.s.connectCall("DELETE", "/connectors/"+x.prefix) // gone: Postgres keeps the WAL in its slot
	time.Sleep(500 * time.Millisecond)
	x.insertRange(100, 200, "while-gone")
	x.bump(1, 50, 9)
	x.write(`DELETE FROM accounts WHERE id BETWEEN 120 AND 140`)
	if v, _ := connectorBodies.Load(x.prefix); v != nil {
		x.s.putConnector(t, x.prefix, v.(string)) // and back, with its slot and offsets intact
	}
	waitRunning(t, x.s, x.prefix)
	x.sync()
}

var _ = cdc.OpCreate
