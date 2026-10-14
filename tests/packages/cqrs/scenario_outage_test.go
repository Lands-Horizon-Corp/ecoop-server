//go:build integration

package cqrs_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// ===========================================================================
// Read-database outage & catch-up sync recovery. The read database goes away;
// the write side keeps changing; Kafka keeps the events. When the read database
// is back, the service replays what it missed and the two databases are equal.
// ===========================================================================

func TestReadOutage_WritesDuringOutageCatchUp(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 50)
	restore := readOutage(t, r.p)
	r.w.bulkInsert(1, 200, "o")
	time.Sleep(300 * time.Millisecond)
	if pm.caughtUp() {
		t.Fatal("nothing can be applied while the read database is gone")
	}
	if s := r.svc.Stats(); s.DeadLettered != 0 {
		t.Fatalf("an outage must never dead-letter: %+v", s)
	}
	restore()
	r.waitInSync()
}

func TestReadOutage_OutageMidStream(t *testing.T) {
	t.Parallel()
	r := newRig(t, 3)
	r.w.startPump(t, r.f, 40)
	r.w.bulkInsert(1, 100, "a")
	r.waitInSync()
	restore := readOutage(t, r.p)
	r.w.bulkUpdate(1, 100, 7)
	r.w.insert(500, "late", 5)
	r.w.remove(10)
	r.w.set(11, "renamed", 99)
	time.Sleep(200 * time.Millisecond)
	restore()
	r.waitInSync()
	if r.svc.Stats().DeadLettered != 0 {
		t.Fatal("nothing may be dead-lettered by an outage")
	}
}

func TestReadOutage_RepeatedFlapping(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.startPump(t, r.f, 30)
	next := int64(1)
	for round := range 6 {
		restore := readOutage(t, r.p)
		r.w.bulkInsert(next, 40, fmt.Sprintf("r%d-", round))
		next += 40
		time.Sleep(120 * time.Millisecond)
		restore()
		r.w.bulkUpdate(1, next-1, 1)
		time.Sleep(80 * time.Millisecond)
	}
	r.waitInSync()
}

func TestReadOutage_BacklogOfUpdatesAndDeletes(t *testing.T) {
	t.Parallel()
	r := newRig(t, 4)
	r.w.startPump(t, r.f, 60)
	r.w.bulkInsert(1, 300, "x")
	r.waitInSync()
	restore := readOutage(t, r.p)
	for range 5 {
		r.w.bulkUpdate(1, 300, 2)
	}
	r.w.bulkRemove(100, 149)
	r.w.bulkInsert(1000, 50, "n")
	restore()
	r.waitInSync()
	if got := len(r.p.readAccounts()); got != 300-50+50 {
		t.Fatalf("rows = %d", got)
	}
}

func TestReadOutage_ReadOnlyThenPromoted(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.startPump(t, r.f, 50)
	r.w.bulkInsert(1, 50, "a")
	r.waitInSync()
	setReadOnly(t, r.p, true)
	killReadBackends(t, r.p, 0)
	r.w.bulkUpdate(1, 50, 4)
	r.w.bulkInsert(100, 50, "b")
	time.Sleep(250 * time.Millisecond)
	if d, err := r.p.syncDiff(); err == nil && d == "" {
		t.Fatal("a read-only read database cannot have taken the changes")
	}
	setReadOnly(t, r.p, false)
	killReadBackends(t, r.p, 0)
	r.waitInSync()
}

func TestReadOutage_TimeoutsAreRetriedNotDeadLettered(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1, func(c *cqrsSvc) { c.ApplyTimeout = 300 * time.Millisecond })
	r.w.startPump(t, r.f, 50)
	r.w.bulkInsert(1, 20, "a")
	r.waitInSync()
	_, release := holdRead(t, r.p, `LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE`)
	r.w.bulkUpdate(1, 20, 3)
	eventuallyN(t, 20*time.Second, "timeouts to be retried", func() bool { return r.svc.Stats().Retries >= 2 })
	if r.svc.Stats().DeadLettered != 0 || len(r.f.deadLettered()) != 0 {
		t.Fatal("a lock timeout is transient: nothing may be dead-lettered")
	}
	release()
	r.waitInSync()
}

func TestReadOutage_NothingIsCommittedWhileDown(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.startPump(t, r.f, 50)
	r.w.bulkInsert(1, 30, "a")
	r.waitInSync()
	cp, kafka := r.p.checkpoint(r.group, 0), r.f.committedOffset(0)
	restore := readOutage(t, r.p)
	r.w.bulkUpdate(1, 30, 2)
	time.Sleep(400 * time.Millisecond)
	if got := r.f.committedOffset(0); got != kafka {
		t.Fatalf("Kafka offset moved from %d to %d without anything being applied", kafka, got)
	}
	restore()
	r.waitInSync()
	if r.p.checkpoint(r.group, 0) <= cp || r.f.committedOffset(0) <= kafka {
		t.Fatal("both positions must advance once the read database is back")
	}
}

func TestReadOutage_HooksFireOnceAfterRecovery(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	r := newRig(t, 2, withHooks(rec))
	r.w.startPump(t, r.f, 25)
	restore := readOutage(t, r.p)
	r.w.bulkInsert(1, 50, "h")
	time.Sleep(200 * time.Millisecond)
	if rec.count() != 0 {
		t.Fatal("a hook must not fire for a change that is not in the read database yet")
	}
	restore()
	r.waitInSync()
	eventuallyN(t, 15*time.Second, "50 hook events", func() bool { return rec.count() == 50 })
	time.Sleep(300 * time.Millisecond)
	if rec.count() != 50 {
		t.Fatalf("hooks fired %d times for 50 changes", rec.count())
	}
}

func TestReadOutage_RunRefusesWhileDownThenCatchesUp(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	w := newWorld(t, p, 2)
	f := newFakeCDC("g-down-run")
	svc := newService(t, p, f)
	w.bulkInsert(1, 80, "a")
	restore := readOutage(t, p)
	if err := svc.Run(longCtx(t)); !errors.Is(err, cqrs.ErrReadDBUnreachable) {
		t.Fatalf("Run while the read database is down = %v", err)
	}
	restore()
	start(t, svc)
	pm := w.startPump(t, f, 40)
	eventuallyN(t, 60*time.Second, "the backlog", pm.caughtUp)
	p.assertInSync(t)
}

func TestReadOutage_PerRowOrderAcrossPartitions(t *testing.T) {
	t.Parallel()
	r := newRig(t, 4)
	r.w.startPump(t, r.f, 33)
	r.w.bulkInsert(1, 40, "o")
	r.waitInSync()
	restore := readOutage(t, r.p)
	for range 20 {
		r.w.bulkUpdate(1, 40, 1)
	}
	restore()
	r.waitInSync()
	for id, a := range r.p.readAccounts() {
		if a.Version != 21 || a.Balance != id+20 {
			t.Fatalf("row %d = %+v: the 20 updates must apply in order", id, a)
		}
	}
}

func TestReadOutage_KilledRepeatedlyDuringALargeBatch(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.bulkInsert(1, 4000, "big")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(25 * time.Millisecond):
				killReadBackends(t, r.p, 0)
			}
		}
	}()
	r.w.startPump(t, r.f, 1000)
	time.Sleep(1500 * time.Millisecond)
	close(stop)
	wg.Wait()
	r.waitInSync()
	if got := r.p.checkpoint(r.group, 0); got != 4000 {
		t.Fatalf("checkpoint = %d", got)
	}
}

func TestReadOutage_OutageAtTheVeryStartOfACatchUp(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.bulkInsert(1, 500, "pre")
	restore := readOutage(t, r.p)
	pm := r.w.startPump(t, r.f, 100)
	time.Sleep(300 * time.Millisecond)
	if pm.handled.Load() != 0 {
		t.Fatal("nothing can be handled while the read database is down")
	}
	restore()
	r.waitInSync()
}

// ===========================================================================
// Kafka broker downtime while the write database keeps changing. The consumer
// cannot fetch or commit; on recovery it resumes from its committed offsets and
// replays everything that accumulated.
// ===========================================================================

func TestBrokerDown_WritesDuringDowntimeAreReplayed(t *testing.T) {
	t.Parallel()
	r := newRig(t, 3)
	pm := r.w.startPump(t, r.f, 50)
	r.w.bulkInsert(1, 100, "a")
	r.waitInSync()
	pm.down.Store(true)
	r.w.bulkUpdate(1, 100, 3)
	r.w.bulkInsert(200, 100, "b")
	time.Sleep(150 * time.Millisecond)
	if d, _ := r.p.syncDiff(); d == "" {
		t.Fatal("with the broker down the read database must be behind")
	}
	pm.down.Store(false)
	r.waitInSync()
}

func TestBrokerDown_ResumesFromCommittedNeverBackwards(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 20)
	var seen [2]atomic.Int64
	var regressed atomic.Bool
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				for part := range int32(2) {
					c := r.f.committedOffset(part)
					if c < seen[part].Load() {
						regressed.Store(true)
					}
					seen[part].Store(c)
				}
			}
		}
	}()
	t.Cleanup(func() { close(stop) })
	r.w.bulkInsert(1, 200, "a")
	time.Sleep(100 * time.Millisecond)
	pm.down.Store(true)
	r.w.bulkUpdate(1, 200, 1)
	time.Sleep(100 * time.Millisecond)
	pm.down.Store(false)
	r.waitInSync()
	if regressed.Load() {
		t.Fatal("the committed offset went backwards across the downtime")
	}
}

func TestBrokerDown_LongDowntimeBigBacklog(t *testing.T) {
	t.Parallel()
	r := newRig(t, 4)
	pm := r.w.startPump(t, r.f, 200)
	pm.down.Store(true)
	r.w.bulkInsert(1, 3000, "q")
	r.w.bulkUpdate(1, 3000, 5)
	r.w.bulkRemove(2000, 2400)
	time.Sleep(200 * time.Millisecond)
	pm.down.Store(false)
	r.waitInSync()
}

func TestBrokerDown_RepeatedDowntime(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 25)
	next := int64(1)
	for range 6 {
		pm.down.Store(true)
		r.w.bulkInsert(next, 30, "d")
		next += 30
		time.Sleep(100 * time.Millisecond)
		pm.down.Store(false)
		r.w.bulkUpdate(1, next-1, 2)
		time.Sleep(60 * time.Millisecond)
	}
	r.waitInSync()
}

func TestBrokerDown_DeletesAndRecreatesWhileDown(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 30)
	r.w.bulkInsert(1, 60, "a")
	r.waitInSync()
	pm.down.Store(true)
	r.w.bulkRemove(1, 30)
	r.w.bulkInsert(1, 15, "again")
	r.w.bulkUpdate(5, 10, 100)
	time.Sleep(100 * time.Millisecond)
	pm.down.Store(false)
	r.waitInSync()
}

func TestBrokerDown_DownBeforeAnythingWasConsumed(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 40)
	pm.down.Store(true)
	r.w.bulkInsert(1, 150, "first")
	time.Sleep(150 * time.Millisecond)
	if len(r.p.readAccounts()) != 0 {
		t.Fatal("nothing can have arrived")
	}
	pm.down.Store(false)
	r.waitInSync()
}

func TestBrokerDown_CommitsDoNotAdvanceWhileDown(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	pm := r.w.startPump(t, r.f, 50)
	r.w.bulkInsert(1, 50, "a")
	r.waitInSync()
	before := r.f.committedOffset(0)
	pm.down.Store(true)
	r.w.bulkUpdate(1, 50, 1)
	time.Sleep(250 * time.Millisecond)
	if got := r.f.committedOffset(0); got != before {
		t.Fatalf("committed offset moved %d -> %d while the broker was down", before, got)
	}
	pm.down.Store(false)
	r.waitInSync()
	if r.f.committedOffset(0) != r.w.size(0) {
		t.Fatal("after recovery the consumer must be committed to the end of the log")
	}
}

func TestBrokerDown_ReadDatabaseStaysConsistentWhileBehind(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 50)
	r.w.bulkInsert(1, 80, "a")
	r.waitInSync()
	snapshot := r.p.readAccounts()
	pm.down.Store(true)
	r.w.bulkUpdate(1, 80, 9)
	for range 5 {
		time.Sleep(60 * time.Millisecond)
		if d := diffAccounts(snapshot, r.p.readAccounts(), nil); d != "" {
			t.Fatalf("the read database changed while the broker was down: %s", d)
		}
	}
	pm.down.Store(false)
	r.waitInSync()
}

func TestBrokerDown_HooksFireOncePerChangeAcrossDowntime(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	r := newRig(t, 2, withHooks(rec))
	pm := r.w.startPump(t, r.f, 20)
	r.w.bulkInsert(1, 40, "a")
	r.waitInSync()
	pm.down.Store(true)
	r.w.bulkUpdate(1, 40, 1)
	time.Sleep(100 * time.Millisecond)
	pm.down.Store(false)
	r.waitInSync()
	eventuallyN(t, 15*time.Second, "80 hook events", func() bool { return rec.count() == 80 })
	time.Sleep(300 * time.Millisecond)
	if rec.count() != 80 {
		t.Fatalf("hooks fired %d times for 80 changes", rec.count())
	}
}

func TestBrokerDown_PoisonAndDeadLetterOutageTogether(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.p.exec(r.p.read, `ALTER TABLE accounts ADD CONSTRAINT balance_ok CHECK (balance >= 0)`)
	pm := r.w.startPump(t, r.f, 50)
	pm.down.Store(true)
	r.w.bulkInsert(1, 20, "ok")
	r.w.insert(21, "poison", -5)
	r.w.bulkInsert(30, 20, "ok")
	r.f.failDeadLetter(errors.New("dlq down too"))
	pm.down.Store(false)
	time.Sleep(400 * time.Millisecond)
	if len(r.f.deadLettered()) != 0 {
		t.Fatal("nothing can be parked while the dead-letter topic is down")
	}
	r.f.failDeadLetter(nil)
	r.waitInSync(21)
	if d := r.f.deadLettered(); len(d) != 1 {
		t.Fatalf("the poison record must be parked exactly once, got %d", len(d))
	}
}

func TestBrokerDown_PerRowOrderAfterRecovery(t *testing.T) {
	t.Parallel()
	r := newRig(t, 4)
	pm := r.w.startPump(t, r.f, 17)
	r.w.bulkInsert(1, 60, "a")
	r.waitInSync()
	pm.down.Store(true)
	for range 15 {
		r.w.bulkUpdate(1, 60, 1)
	}
	pm.down.Store(false)
	r.waitInSync()
	for id, a := range r.p.readAccounts() {
		if a.Version != 16 || a.Balance != id+15 {
			t.Fatalf("row %d = %+v", id, a)
		}
	}
}
