//go:build integration

package cqrs_test

import (
	"errors"
	"slices"
	"testing"
	"time"
)

// ===========================================================================
// Offset commit failure after a successful database transaction. The rows and
// the checkpoint are committed in the read database; telling Kafka then fails.
// Kafka delivers the same records again, and applying them again must change
// nothing.
// ===========================================================================

func TestCommitFailure_RedeliveriesAreSkippedAndDatabasesStayEqual(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 30)
	pm.failCommit.Store(true)
	r.w.bulkInsert(1, 120, "a")
	r.w.bulkUpdate(1, 120, 2)
	eventuallyN(t, 30*time.Second, "redeliveries", func() bool { return r.svc.Stats().Duplicates > 100 })
	r.waitInSync()
	if r.svc.Stats().CommitFails == 0 {
		t.Fatal("the commit failures must be visible in the stats")
	}
	pm.failCommit.Store(false)
	r.waitInSync()
	eventuallyN(t, 15*time.Second, "kafka to catch up", pm.caughtUp)
}

func TestCommitFailure_AppliedExactlyOnce(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	pm := r.w.startPump(t, r.f, 25)
	pm.failCommit.Store(true)
	r.w.bulkInsert(1, 100, "a")
	eventuallyN(t, 30*time.Second, "a few redeliveries", func() bool { return r.svc.Stats().Duplicates >= 100 })
	pm.failCommit.Store(false)
	r.waitInSync()
	eventuallyN(t, 15*time.Second, "kafka to catch up", pm.caughtUp)
	if got := r.svc.Stats().Applied; got != 100 {
		t.Fatalf("applied %d rows for 100 changes: some were applied more than once", got)
	}
}

func TestCommitFailure_RedeliveredUpdatesDoNotRegressNewerState(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 60, "a")
	r.w.bulkUpdate(1, 60, 5)
	if err := r.w.deliverNew(ctx, r.f, 40); err != nil {
		t.Fatal(err)
	}
	r.w.bulkUpdate(1, 60, 5)
	if err := r.w.deliverNew(ctx, r.f, 40); err != nil {
		t.Fatal(err)
	}
	// Kafka never saw the commits: everything is delivered again from the start.
	if err := r.w.deliverRange(ctx, r.f, 0, 0, r.w.size(0), 40); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

func TestCommitFailure_RedeliveredDeleteDoesNotResurrectOrRemove(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.insert(1, "first", 1)
	r.w.remove(1)
	r.w.insert(1, "second", 2)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	if err := r.w.deliverRange(ctx, r.f, 0, 0, r.w.size(0), 50); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	if a, ok := r.p.readAccounts()[1]; !ok || a.Owner != "second" {
		t.Fatalf("row 1 = %+v ok=%v", a, ok)
	}
}

func TestCommitFailure_NewConsumerReplaysEverythingAndAppliesNothing(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 100, "a")
	r.w.bulkUpdate(1, 100, 1)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	// The consumer dies before Kafka recorded anything; another instance of the
	// group starts from offset 0.
	f2, svc2 := r.replica()
	if err := r.w.deliverNew(ctx, f2, 50); err != nil {
		t.Fatal(err)
	}
	if s := svc2.Stats(); s.Applied != 0 || s.Duplicates != r.w.total() {
		t.Fatalf("the replacement applied %d and skipped %d, want 0 and %d", s.Applied, s.Duplicates, r.w.total())
	}
	r.p.assertInSync(t)
}

func TestCommitFailure_HooksDoNotRepeat(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	r := newRig(t, 1, withHooks(rec))
	pm := r.w.startPump(t, r.f, 20)
	pm.failCommit.Store(true)
	r.w.bulkInsert(1, 40, "a")
	eventuallyN(t, 30*time.Second, "redeliveries", func() bool { return r.svc.Stats().Duplicates >= 80 })
	pm.failCommit.Store(false)
	r.waitInSync()
	time.Sleep(300 * time.Millisecond)
	if rec.count() != 40 {
		t.Fatalf("hooks fired %d times for 40 changes", rec.count())
	}
}

func TestCommitFailure_IntermittentAcrossPartitions(t *testing.T) {
	t.Parallel()
	r := newRig(t, 4)
	pm := r.w.startPump(t, r.f, 17)
	stop := make(chan struct{})
	go func() {
		on := false
		for {
			select {
			case <-stop:
				pm.failCommit.Store(false)
				return
			case <-time.After(30 * time.Millisecond):
				on = !on
				pm.failCommit.Store(on)
			}
		}
	}()
	t.Cleanup(func() { close(stop) })
	next := int64(1)
	for range 6 {
		r.w.bulkInsert(next, 50, "i")
		next += 50
		r.w.bulkUpdate(1, next-1, 1)
		time.Sleep(40 * time.Millisecond)
	}
	r.waitInSync()
}

func TestCommitFailure_CheckpointIsAheadOfKafkaNotBehind(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	pm := r.w.startPump(t, r.f, 50)
	pm.failCommit.Store(true)
	r.w.bulkInsert(1, 100, "a")
	r.waitInSync()
	if cp, kafka := r.p.checkpoint(r.group, 0), r.f.committedOffset(0); cp <= kafka {
		t.Fatalf("checkpoint %d must be ahead of Kafka's %d: the database is what is durable", cp, kafka)
	}
}

func TestCommitFailure_CrashRightAfterTransaction(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 200, "a")
	for part := range r.w.parts {
		batch := r.w.slice(part, 0, 60)
		if err := r.f.call(ctx, batch); err != nil { // applied; the process dies before committing
			t.Fatal(err)
		}
	}
	f2, _ := r.replica()
	if err := r.w.deliverNew(ctx, f2, 60); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

func TestCommitFailure_PoisonIsNotParkedAgainOnRedelivery(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.p.exec(r.p.read, `ALTER TABLE accounts ADD CONSTRAINT balance_ok CHECK (balance >= 0)`)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 10, "a")
	r.w.insert(11, "bad", -3)
	r.w.bulkInsert(20, 10, "b")
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	if err := r.w.deliverRange(ctx, r.f, 0, 0, r.w.size(0), 50); err != nil {
		t.Fatal(err)
	}
	if err := r.w.deliverRange(ctx, r.f, 0, 0, r.w.size(0), 7); err != nil {
		t.Fatal(err)
	}
	if got := len(r.f.deadLettered()); got != 1 {
		t.Fatalf("the poison record was parked %d times", got)
	}
	r.p.assertInSync(t, 11)
}

func TestCommitFailure_HeavyRedeliveryOfALargeBacklog(t *testing.T) {
	t.Parallel()
	r := newRig(t, 3)
	pm := r.w.startPump(t, r.f, 200)
	pm.failCommit.Store(true)
	r.w.bulkInsert(1, 2000, "big")
	r.w.bulkUpdate(1, 2000, 1)
	time.Sleep(1500 * time.Millisecond)
	pm.failCommit.Store(false)
	r.waitInSync()
	if got := r.svc.Stats().Applied; got != 4000 {
		t.Fatalf("applied %d, want 4000", got)
	}
}

// ===========================================================================
// Partial batch failure with DLQ isolation. Rows the read database rejects for
// good are parked on the dead-letter topic one by one; every other event in the
// same batches keeps syncing.
// ===========================================================================

func badCheck(r *rig) {
	r.p.exec(r.p.read, `ALTER TABLE accounts ADD CONSTRAINT balance_ok CHECK (balance >= 0)`)
}

func parkedOffsets(f *fakeCDC) []int64 {
	var out []int64
	for _, d := range f.deadLettered() {
		out = append(out, d.Offset)
	}
	slices.Sort(out)
	return out
}

func TestDLQIsolation_OneBadRowAmongMany(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	badCheck(r)
	r.w.bulkInsert(1, 50, "a")
	r.w.insert(51, "bad", -1)
	r.w.bulkInsert(60, 49, "b")
	if err := r.w.deliverNew(longCtx(t), r.f, 200); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t, 51)
	if _, ok := r.p.readAccounts()[51]; ok {
		t.Fatal("the rejected row must not be in the read database")
	}
	if got := parkedOffsets(r.f); len(got) != 1 || got[0] != 50 {
		t.Fatalf("parked = %v", got)
	}
}

func TestDLQIsolation_EveryTenthRowIsBad(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	badCheck(r)
	var bad []int64
	for i := int64(1); i <= 120; i++ {
		if i%10 == 0 {
			r.w.insert(i, "bad", -i)
			bad = append(bad, i)
		} else {
			r.w.insert(i, "ok", i)
		}
	}
	if err := r.w.deliverNew(longCtx(t), r.f, 40); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t, bad...)
	if got := len(r.f.deadLettered()); got != len(bad) {
		t.Fatalf("parked %d, want %d", got, len(bad))
	}
	if got := len(r.p.readAccounts()); got != 120-len(bad) {
		t.Fatalf("read rows = %d", got)
	}
}

func TestDLQIsolation_AWholeBatchOfBadRows(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	badCheck(r)
	var bad []int64
	for i := int64(1); i <= 30; i++ {
		r.w.insert(i, "bad", -i)
		bad = append(bad, i)
	}
	r.w.bulkInsert(100, 10, "ok")
	if err := r.w.deliverNew(longCtx(t), r.f, 100); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t, bad...)
	if len(r.f.deadLettered()) != 30 || r.p.checkpoint(r.group, 0) != 40 {
		t.Fatalf("parked %d, checkpoint %d", len(r.f.deadLettered()), r.p.checkpoint(r.group, 0))
	}
}

func TestDLQIsolation_BadFirstAndBadLast(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	badCheck(r)
	r.w.insert(1, "bad-first", -1)
	r.w.bulkInsert(10, 20, "ok")
	r.w.insert(99, "bad-last", -2)
	if err := r.w.deliverNew(longCtx(t), r.f, 100); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t, 1, 99)
	if got := parkedOffsets(r.f); !slices.Equal(got, []int64{0, 21}) {
		t.Fatalf("parked = %v", got)
	}
}

func TestDLQIsolation_BadUpdateOfAnExistingRow(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	badCheck(r)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 20, "a")
	if err := r.w.deliverNew(ctx, r.f, 100); err != nil {
		t.Fatal(err)
	}
	r.w.set(5, "now-bad", -9)
	r.w.bulkUpdate(10, 15, 1)
	if err := r.w.deliverNew(ctx, r.f, 100); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t, 5)
	if a := r.p.readAccounts()[5]; a.Balance != 5 || a.Version != 1 {
		t.Fatalf("a rejected update must leave the old row as it was: %+v", a)
	}
	if len(r.f.deadLettered()) != 1 {
		t.Fatalf("parked %d", len(r.f.deadLettered()))
	}
}

func TestDLQIsolation_UndecodableAndRejectedTogether(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	badCheck(r)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 10, "a")
	r.w.insert(11, "bad", -1)
	r.w.bulkInsert(20, 10, "b")
	// A garbage record, in its own position, in the same batch as the rejected row.
	garbage := poison(0, r.w.size(0))
	r.w.mu.Lock()
	r.w.log[0] = append(r.w.log[0], garbage)
	r.w.mu.Unlock()
	if err := r.w.deliverNew(ctx, r.f, 100); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t, 11)
	if got := len(r.f.deadLettered()); got != 2 {
		t.Fatalf("parked %d, want the rejected row and the garbage record", got)
	}
}

func TestDLQIsolation_BadRowsInEveryPartition(t *testing.T) {
	t.Parallel()
	r := newRig(t, 4)
	badCheck(r)
	var bad []int64
	for i := int64(1); i <= 80; i++ {
		if i%7 == 0 {
			r.w.insert(i, "bad", -1)
			bad = append(bad, i)
		} else {
			r.w.insert(i, "ok", i)
		}
	}
	if err := r.w.deliverNew(longCtx(t), r.f, 30); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t, bad...)
	for _, d := range r.f.deadLettered() {
		if d.Partition < 0 || d.Partition > 3 || len(d.Key) == 0 {
			t.Fatalf("a dead letter must say where it came from: %+v", d)
		}
	}
}

func TestDLQIsolation_BadThenGoodChangeOfTheSameKeyInOneBatch(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	badCheck(r)
	r.w.insert(1, "v1", 10)
	r.w.set(1, "bad", -5)
	r.w.set(1, "good-again", 30)
	if err := r.w.deliverNew(longCtx(t), r.f, 100); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	if a := r.p.readAccounts()[1]; a.Owner != "good-again" || a.Balance != 30 {
		t.Fatalf("row = %+v", a)
	}
}

func TestDLQIsolation_DeadLetterOutageStallsThenRecovers(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	badCheck(r)
	r.f.failDeadLetter(errors.New("dlq down"))
	r.w.bulkInsert(1, 20, "a")
	r.w.insert(21, "bad", -1)
	r.w.bulkInsert(30, 20, "b")
	pm := r.w.startPump(t, r.f, 100)
	time.Sleep(500 * time.Millisecond)
	if pm.caughtUp() || len(r.f.deadLettered()) != 0 {
		t.Fatal("the partition must stall while the poison cannot be parked")
	}
	r.f.failDeadLetter(nil)
	r.waitInSync(21)
	if got := len(r.f.deadLettered()); got != 1 {
		t.Fatalf("parked %d", got)
	}
}

func TestDLQIsolation_ValidCatchUpEventsKeepFlowingAroundPoison(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	badCheck(r)
	pm := r.w.startPump(t, r.f, 25)
	restore := readOutage(t, r.p)
	var bad []int64
	for i := int64(1); i <= 200; i++ {
		if i%25 == 0 {
			r.w.insert(i, "bad", -1)
			bad = append(bad, i)
		} else {
			r.w.insert(i, "ok", i)
		}
	}
	restore()
	r.waitInSync(bad...)
	eventuallyN(t, 20*time.Second, "kafka to catch up", pm.caughtUp)
	if got := len(r.f.deadLettered()); got != len(bad) {
		t.Fatalf("parked %d, want %d", got, len(bad))
	}
}

func TestDLQIsolation_NoStallWhenOnlyPoisonRemains(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	badCheck(r)
	pm := r.w.startPump(t, r.f, 10)
	for i := int64(1); i <= 25; i++ {
		r.w.insert(i, "bad", -i)
	}
	eventuallyN(t, 30*time.Second, "all of it to be parked", func() bool { return len(r.f.deadLettered()) == 25 && pm.caughtUp() })
	if r.p.count(r.p.read, "accounts") != 0 {
		t.Fatal("no row may have been written")
	}
}
