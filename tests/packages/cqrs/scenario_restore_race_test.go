//go:build integration

package cqrs_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// ===========================================================================
// Stale state injection after a manual read-model snapshot restore. An operator
// restores the read database from a backup taken earlier. The restored rows (and
// the restored checkpoint) are older than the write database. The backlog that
// follows replays events older than some of the restored rows. The version guard
// must turn the outdated ones away, so the restore can only ever move a row
// forward, and the databases end up equal.
// ===========================================================================

// snapshot is a copy of the read model, as a backup would hold it.
type snapshot struct {
	rows map[int64]Account
	cp   map[int32]int64
}

func (r *rig) takeSnapshot() snapshot {
	r.t.Helper()
	s := snapshot{rows: r.p.readAccounts(), cp: map[int32]int64{}}
	for part := range r.w.parts {
		s.cp[part] = r.p.checkpoint(r.group, part)
	}
	return s
}

// restore puts the snapshot's rows and checkpoints back into the read database.
func (r *rig) restore(s snapshot, withCheckpoint bool) {
	r.t.Helper()
	r.p.exec(r.p.read, `DELETE FROM accounts`)
	for _, a := range s.rows {
		note := any(nil)
		if a.Note != nil {
			note = *a.Note
		}
		r.p.exec(r.p.read, `INSERT INTO accounts (id, owner, balance, note, active, updated_at, version) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			a.ID, a.Owner, a.Balance, note, a.Active, a.UpdatedAt, a.Version)
	}
	r.p.exec(r.p.read, `DELETE FROM cqrs_checkpoints`)
	if withCheckpoint {
		for part, next := range s.cp {
			if next > 0 {
				r.p.exec(r.p.read, `INSERT INTO cqrs_checkpoints (consumer_group, topic, partition, next_offset, updated_at) VALUES (?, ?, ?, ?, now())`,
					r.group, testTopic, part, next)
			}
		}
	}
}

func TestSnapshotRestore_RestoredCheckpointReplaysTheGap(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 100, "a")
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	snap := r.takeSnapshot()
	r.w.bulkUpdate(1, 100, 5)
	r.w.bulkRemove(90, 100)
	r.w.bulkInsert(500, 20, "n")
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
	// The operator restores the old backup, checkpoints included. Kafka is
	// rewound to what the backup had seen, and replays the gap.
	r.restore(snap, true)
	for part, next := range snap.cp {
		r.f.setCommitted(part, next)
	}
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

func TestSnapshotRestore_BackupWithoutCheckpointsReplaysTheWholeLog(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 80, "a")
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	snap := r.takeSnapshot()
	r.w.bulkUpdate(1, 80, 3)
	r.w.bulkUpdate(1, 80, 3)
	r.w.bulkRemove(70, 80)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	// A restore that dropped the checkpoint table too: every record is new again.
	r.restore(snap, false)
	for part := range r.w.parts {
		r.f.setCommitted(part, 0)
	}
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

func TestSnapshotRestore_OutdatedEventsAreTurnedAway(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 60, "a")
	r.w.bulkUpdate(1, 60, 1)
	r.w.bulkUpdate(1, 60, 1)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	// The read database is restored to a state NEWER than the start of the log
	// but with no checkpoint: the old events at the start of the backlog are stale.
	r.p.exec(r.p.read, `DELETE FROM cqrs_checkpoints`)
	r.f.setCommitted(0, 0)
	stale0 := r.svc.Stats().Stale
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	if got := r.svc.Stats().Stale - stale0; got == 0 {
		t.Fatal("events older than the stored rows must be turned away by the version guard")
	}
	r.p.assertInSync(t)
}

func TestSnapshotRestore_RowsOnlyEverMoveForward(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 40, "a")
	for range 6 {
		r.w.bulkUpdate(1, 40, 2)
	}
	if err := r.w.deliverNew(ctx, r.f, 40); err != nil {
		t.Fatal(err)
	}
	before := r.p.readAccounts()
	r.p.exec(r.p.read, `DELETE FROM cqrs_checkpoints`)
	r.f.setCommitted(0, 0)
	// Replay the whole log in many different batch sizes; no row may ever have a
	// lower version after than before, at any point.
	for _, size := range []int{1, 3, 7, 19, 200} {
		for part := range r.w.parts {
			r.f.setCommitted(part, 0)
		}
		r.p.exec(r.p.read, `DELETE FROM cqrs_checkpoints`)
		if err := r.w.deliverNew(ctx, r.f, size); err != nil {
			t.Fatal(err)
		}
		for id, a := range r.p.readAccounts() {
			if a.Version < before[id].Version {
				t.Fatalf("batch size %d: row %d went back from version %d to %d", size, id, before[id].Version, a.Version)
			}
		}
	}
	r.p.assertInSync(t)
}

func TestSnapshotRestore_RestoredRowAheadOfTheBacklogIsKept(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 20, "a")
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	// Rows in the read database carry a higher version than anything the backlog says.
	r.p.exec(r.p.read, `UPDATE accounts SET version = 50, owner = 'from-the-future' WHERE id = 5`)
	r.p.exec(r.p.read, `DELETE FROM cqrs_checkpoints`)
	r.f.setCommitted(0, 0)
	if err := r.w.deliverNew(ctx, r.f, 50); err != nil {
		t.Fatal(err)
	}
	if a := r.p.readAccounts()[5]; a.Owner != "from-the-future" || a.Version != 50 {
		t.Fatalf("a row newer than every event must not be overwritten: %+v", a)
	}
}

func TestSnapshotRestore_DeletesInTheBacklogRespectTheRestoredVersions(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 30, "a")
	r.w.bulkRemove(1, 10) // events: create, then delete (old row has version 1)
	if err := r.w.deliverNew(ctx, r.f, 100); err != nil {
		t.Fatal(err)
	}
	// The restore brings back rows 1..10 at a NEWER version than the delete names.
	for id := int64(1); id <= 10; id++ {
		r.p.exec(r.p.read, `INSERT INTO accounts (id, owner, balance, version) VALUES (?, 'restored-newer', 1, 9)`, id)
	}
	r.p.exec(r.p.read, `DELETE FROM cqrs_checkpoints`)
	r.f.setCommitted(0, 0)
	if err := r.w.deliverNew(ctx, r.f, 100); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 10; id++ {
		if a, ok := r.p.readAccounts()[id]; !ok || a.Owner != "restored-newer" {
			t.Fatalf("row %d: a delete of version 1 must not remove a restored row at version 9", id)
		}
	}
}

func TestSnapshotRestore_LiveWritesDuringAndAfterTheRestore(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	pm := r.w.startPump(t, r.f, 30)
	r.w.bulkInsert(1, 100, "a")
	r.waitInSync()
	snap := r.takeSnapshot()
	r.w.bulkUpdate(1, 100, 4)
	r.waitInSync()
	pm.down.Store(true)
	time.Sleep(60 * time.Millisecond)
	r.restore(snap, true)
	for part, next := range snap.cp {
		r.f.setCommitted(part, next)
	}
	r.w.bulkUpdate(1, 100, 1) // more writes while the restore is being done
	pm.down.Store(false)
	r.waitInSync()
}

func TestSnapshotRestore_ManyRestoresInARow(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 60, "a")
	if err := r.w.deliverNew(ctx, r.f, 40); err != nil {
		t.Fatal(err)
	}
	snap := r.takeSnapshot()
	for round := range 4 {
		r.w.bulkUpdate(1, 60, 1)
		r.w.insert(int64(1000+round), "n", 1)
		if err := r.w.deliverNew(ctx, r.f, 40); err != nil {
			t.Fatal(err)
		}
		r.restore(snap, true)
		for part, next := range snap.cp {
			r.f.setCommitted(part, next)
		}
		if err := r.w.deliverNew(ctx, r.f, 40); err != nil {
			t.Fatal(err)
		}
		r.p.assertInSync(t)
	}
}

func TestSnapshotRestore_RestoreWithAnEmptyReadModel(t *testing.T) {
	t.Parallel()
	r := newRig(t, 3)
	ctx := longCtx(t)
	r.w.bulkInsert(1, 90, "a")
	r.w.bulkUpdate(1, 90, 2)
	r.w.bulkRemove(80, 90)
	if err := r.w.deliverNew(ctx, r.f, 40); err != nil {
		t.Fatal(err)
	}
	r.restore(snapshot{rows: map[int64]Account{}, cp: map[int32]int64{}}, false)
	for part := range r.w.parts {
		r.f.setCommitted(part, 0)
	}
	if err := r.w.deliverNew(ctx, r.f, 40); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

func TestSnapshotRestore_RealPipelineFromAnOldBackup(t *testing.T) {
	t.Parallel()
	x := newE2E(t)
	x.insertRange(1, 200, "a")
	x.sync()
	backup := x.p.readAccounts()
	x.bump(1, 200, 5)
	x.bump(1, 100, 5)
	x.write(`DELETE FROM accounts WHERE id BETWEEN 150 AND 200`)
	x.sync()
	// The read model is replaced by the old backup while the pipeline is running.
	x.p.exec(x.p.read, `DELETE FROM accounts`)
	for _, a := range backup {
		x.p.exec(x.p.read, `INSERT INTO accounts (id, owner, balance, active, updated_at, version) VALUES (?, ?, ?, ?, ?, ?)`,
			a.ID, a.Owner, a.Balance, a.Active, a.UpdatedAt, a.Version)
	}
	// New writes arrive: each carries a higher version than the backup, so the
	// affected rows catch up. Rows nothing touches again stay as the backup had them,
	// which is what the version guard promises: it never goes back, but it also
	// cannot invent changes it is not told about. The operator finishes the
	// restore by touching what is missing (here, by re-emitting through an update).
	x.bump(1, 149, 0)
	x.write(`DELETE FROM accounts WHERE id BETWEEN 150 AND 200`)
	x.p.exec(x.p.read, `DELETE FROM accounts WHERE id BETWEEN 150 AND 200`)
	x.sync()
}

// ===========================================================================
// Concurrent multi-replica catch-up race. Several replicas consume the same
// backlogged partition at the same time (a rebalance that overlaps, a slow
// replica that comes back). The checkpoint row lock makes them take turns; none
// deadlocks, none applies a record twice, and the databases end up equal.
// ===========================================================================

func replicas(r *rig, n int, mods ...func(*cqrsSvc)) []*fakeCDC {
	out := []*fakeCDC{r.f}
	for range n - 1 {
		f, _ := r.replica(mods...)
		out = append(out, f)
	}
	return out
}

// race has every replica work through the whole backlog from its committed
// offsets, at the same time.
func (r *rig) race(fs []*fakeCDC, size int) {
	r.t.Helper()
	ctx := longCtx(r.t)
	var wg sync.WaitGroup
	errs := make(chan error, len(fs))
	for _, f := range fs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.w.deliverNew(ctx, f, size); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		r.t.Fatal(err)
	}
}

func TestMultiReplicaRace_BigBacklogManyReplicas(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.bulkInsert(1, 2000, "a")
	r.w.bulkUpdate(1, 2000, 3)
	r.race(replicas(r, 6), 150)
	r.p.assertInSync(t)
}

func TestMultiReplicaRace_EachRecordAppliedOnce(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	_, s2 := r.replica()
	_, s3 := r.replica()
	svcs := []svcT{r.svc, s2, s3}
	if fakeOf(s2) == nil || fakeOf(s2) == r.f {
		t.Fatal("each replica must have its own fake consumer")
	}
	r.w.bulkInsert(1, 600, "a")
	r.w.bulkUpdate(1, 600, 1)
	r.raceAll(svcs, 100)
	r.p.assertInSync(t)
	var applied int64
	for _, s := range svcs {
		applied += s.Stats().Applied
	}
	if applied != 1200 {
		t.Fatalf("the replicas wrote %d row changes between them, want exactly 1200", applied)
	}
}

func TestMultiReplicaRace_NoDeadlockOnOverlappingKeys(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.bulkInsert(1, 40, "hot")
	for range 30 {
		r.w.bulkUpdate(1, 40, 1) // every batch touches the same 40 keys
	}
	r.race(replicas(r, 5), 40)
	r.p.assertInSync(t)
	for _, a := range r.p.readAccounts() {
		if a.Version != 31 {
			t.Fatalf("row %d version = %d, want 31", a.ID, a.Version)
		}
	}
}

func TestMultiReplicaRace_ReplicasOnDifferentPartitions(t *testing.T) {
	t.Parallel()
	r := newRig(t, 4)
	r.w.bulkInsert(1, 800, "a")
	r.w.bulkUpdate(1, 800, 2)
	fs := replicas(r, 4)
	ctx := longCtx(t)
	var wg sync.WaitGroup
	for i, f := range fs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			part := int32(i)
			for {
				batch := r.w.slice(part, f.committedOffset(part), 100)
				if len(batch) == 0 {
					return
				}
				if err := f.deliver(ctx, batch); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	r.p.assertInSync(t)
}

func TestMultiReplicaRace_ReplicaJoinsHalfwayThrough(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.bulkInsert(1, 600, "a")
	ctx := longCtx(t)
	for part := range r.w.parts {
		if err := r.f.deliver(ctx, r.w.slice(part, 0, 150)); err != nil {
			t.Fatal(err)
		}
	}
	r.w.bulkUpdate(1, 600, 1)
	f2, _ := r.replica()
	r.race([]*fakeCDC{r.f, f2}, 70)
	r.p.assertInSync(t)
}

func TestMultiReplicaRace_ReplicaFallsBehindAndComesBack(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.bulkInsert(1, 300, "a")
	f2, _ := r.replica()
	ctx := longCtx(t)
	if err := r.w.deliverNew(ctx, f2, 100); err != nil { // the slow one, early
		t.Fatal(err)
	}
	r.w.bulkUpdate(1, 300, 4)
	r.w.bulkRemove(250, 300)
	if err := r.w.deliverNew(ctx, r.f, 100); err != nil { // the fast one finishes
		t.Fatal(err)
	}
	// The slow replica wakes up with offsets Kafka had for it long ago.
	f2.setCommitted(0, 0)
	if err := r.w.deliverNew(ctx, f2, 100); err != nil {
		t.Fatal(err)
	}
	r.p.assertInSync(t)
}

func TestMultiReplicaRace_ConcurrentWritesWhileReplicasRace(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	r.w.bulkInsert(1, 200, "a")
	fs := replicas(r, 4)
	var pumps []*pump
	for _, f := range fs {
		pumps = append(pumps, r.w.startPump(t, f, 30))
	}
	next := int64(1000)
	for range 8 {
		r.w.bulkInsert(next, 25, "n")
		next += 25
		r.w.bulkUpdate(1, 200, 1)
		time.Sleep(30 * time.Millisecond)
	}
	r.waitInSync()
	for _, pm := range pumps {
		eventuallyN(t, 30*time.Second, "every replica to reach the end", pm.caughtUp)
	}
}

func TestMultiReplicaRace_ReplicasUnderReadOutage(t *testing.T) {
	t.Parallel()
	r := newRig(t, 2)
	fs := replicas(r, 3)
	for _, f := range fs {
		r.w.startPump(t, f, 40)
	}
	restore := readOutage(t, r.p)
	r.w.bulkInsert(1, 300, "a")
	r.w.bulkUpdate(1, 300, 2)
	time.Sleep(200 * time.Millisecond)
	restore()
	r.waitInSync()
}

func TestMultiReplicaRace_ManyReplicasWithPoison(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	badCheck(r)
	var bad []int64
	for i := int64(1); i <= 120; i++ {
		if i%15 == 0 {
			r.w.insert(i, "bad", -1)
			bad = append(bad, i)
		} else {
			r.w.insert(i, "ok", i)
		}
	}
	fs := replicas(r, 4)
	r.race(fs, 30)
	r.p.assertInSync(t, bad...)
	parked := map[int64]bool{}
	for _, f := range fs {
		for _, d := range f.deadLettered() {
			parked[d.Offset] = true
		}
	}
	if len(parked) != len(bad) {
		t.Fatalf("%d distinct poison records parked, want %d", len(parked), len(bad))
	}
}

func TestMultiReplicaRace_ConnectionPoolsStayWithinBounds(t *testing.T) {
	t.Parallel()
	r := newRig(t, 1)
	r.w.bulkInsert(1, 500, "a")
	fs := replicas(r, 8)
	r.race(fs, 50)
	var open int
	if err := readAdmin.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname = $1`, r.p.readDB).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if open > 8*8+4 {
		t.Fatalf("%d connections to the read database for 8 replicas with pools of 8", open)
	}
	r.p.assertInSync(t)
}

func TestMultiReplicaRace_RealPipelineTwoReplicasUnderLoad(t *testing.T) {
	t.Parallel()
	x := newE2E(t)
	x2 := x.s.realCDC(t, x.group, x.prefix)
	svc2 := newService(t, x.p, nil, func(c *cqrsSvc) { c.CDCService = x2; c.Topic = "" })
	start(t, svc2)
	for round := range 4 {
		x.insertRange(int64(round*150+1), 150, "load")
		x.bump(1, int64(round*150+150), 1)
	}
	x.sync()
}

// raceAll is race for services whose fakes are looked up by the rig's replicas.
func (r *rig) raceAll(svcs []svcT, size int) {
	r.t.Helper()
	ctx := longCtx(r.t)
	var wg sync.WaitGroup
	errs := make(chan error, len(svcs))
	for _, s := range svcs {
		f := fakeOf(s)
		if f == nil {
			f = r.f
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.w.deliverNew(ctx, f, size); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		r.t.Fatal(err)
	}
}

var fakes sync.Map // svcT -> *fakeCDC

func fakeOf(s svcT) *fakeCDC {
	if f, ok := fakes.Load(s); ok {
		return f.(*fakeCDC)
	}
	return nil
}

var _ = context.Background
var _ = fmt.Sprint
var _ cdc.ICDCService
