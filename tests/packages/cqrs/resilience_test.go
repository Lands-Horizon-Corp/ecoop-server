//go:build integration

package cqrs_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// ---- graceful shutdown & context cancellation mid-batch --------------------

// Stop while a batch is stuck waiting for the database: Stop returns within its
// own deadline, the stuck batch is not committed, and a new service picks the
// same records up and finishes them with nothing lost or applied twice.
func TestShutdown_StopMidBatchLosesNothing(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	const group = "g-shutdown"
	f1 := newFakeCDC(group)
	svc1 := newService(t, p, f1)
	if err := svc1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Hold a lock the batch needs, so the batch is stuck mid-flight.
	if err := f1.deliver(ctxTimeout(t, time.Minute), []cdc.Record{create(0, 0, 1, "seed", 1, 1)}); err != nil {
		t.Fatal(err)
	}
	_, release := holdRead(t, p, `SELECT * FROM cqrs_checkpoints FOR UPDATE`)

	batch := []cdc.Record{update(0, 1, 1, "after-lock", 2, 2), create(0, 2, 2, "new", 1, 1)}
	runCtx, cancel := context.WithCancel(context.Background())
	stuck := async(func() error { return f1.deliver(runCtx, batch) })
	if !stays(stuck, 500*time.Millisecond) {
		t.Fatal("the batch should be waiting on the lock")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()
	stopped := async(func() error { return svc1.Stop(stopCtx) })
	// Stop is bounded: it must give up waiting rather than hang with the batch.
	if err := within(t, stopped, 10*time.Second, "Stop"); err == nil {
		t.Log("Stop completed (the batch wound down within the deadline)")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v", err)
	}
	cancel()
	release()
	_ = within(t, stuck, 20*time.Second, "the abandoned delivery")

	// The records are not lost: a fresh instance in the same group takes over.
	f2 := newFakeCDC(group)
	start(t, newService(t, p, f2))
	if err := f2.deliver(ctxTimeout(t, time.Minute), batch); err != nil {
		t.Fatal(err)
	}
	got := p.readAccounts()
	if len(got) != 2 || got[1].Owner != "after-lock" || got[2].Owner != "new" {
		t.Fatalf("read db = %+v", got)
	}
	if next := p.checkpoint(group, 0); next != 3 {
		t.Fatalf("checkpoint = %d, want 3", next)
	}
}

// A clean Stop leaves no goroutines behind.
func TestShutdown_NoGoroutineLeak(t *testing.T) {
	// Not parallel: the goroutine count is process-wide.
	p := newPair(t, 8)
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	before := runtime.NumGoroutine()
	for range 5 {
		f := newFakeCDC("g-leak")
		rec := &recorder{}
		svc := newService(t, p, f, func(c *cqrsSvc) {
			c.ToResource = func(a *Account) *AccountResponse { return &AccountResponse{ID: a.ID} }
			c.Created = func(*Account) []string { return []string{"c"} }
			c.Dispatch = rec.dispatch
		})
		if err := svc.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := f.deliver(ctxTimeout(t, time.Minute), seq(0, 0, 1, 10)); err != nil {
			t.Fatal(err)
		}
		if err := svc.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	var after int
	eventually(t, "goroutines to settle", func() bool {
		runtime.GC()
		after = runtime.NumGoroutine()
		return after <= before+3
	})
	t.Logf("goroutines: before=%d after=%d", before, after)
}

// Stop waits for the change hooks that are running, and the hooks are bounded.
func TestShutdown_StopWaitsForHooks(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-hookwait")
	var mu sync.Mutex
	finished := 0
	release := make(chan struct{})
	svc := newService(t, p, f, func(c *cqrsSvc) {
		c.ToResource = func(a *Account) *AccountResponse { return &AccountResponse{ID: a.ID} }
		c.Created = func(*Account) []string { return []string{"c"} }
		c.Dispatch = func(string, []string, *AccountResponse) error {
			<-release
			mu.Lock()
			finished++
			mu.Unlock()
			return nil
		}
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.deliver(ctxTimeout(t, time.Minute), seq(0, 0, 1, 3)); err != nil {
		t.Fatal(err)
	}
	stopped := async(func() error { return svc.Stop(context.Background()) })
	if !stays(stopped, 400*time.Millisecond) {
		t.Fatal("Stop must wait for the hooks that are running")
	}
	close(release)
	if err := within(t, stopped, 10*time.Second, "Stop"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if finished != 3 {
		t.Fatalf("%d hooks finished before Stop returned, want 3", finished)
	}
}

// Hooks past the limit are dropped and counted, never queued without bound.
func TestBackpressure_HooksAreBounded(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-hookcap")
	block := make(chan struct{})
	svc := newService(t, p, f, func(c *cqrsSvc) {
		c.HookWorkers = 2
		c.ToResource = func(a *Account) *AccountResponse { return &AccountResponse{ID: a.ID} }
		c.Created = func(*Account) []string { return []string{"c"} }
		c.Dispatch = func(string, []string, *AccountResponse) error { <-block; return nil }
	})
	start(t, svc)
	t.Cleanup(func() { close(block) })
	if err := f.deliver(ctxTimeout(t, time.Minute), seq(0, 0, 1, 20)); err != nil {
		t.Fatal(err)
	}
	s := svc.Stats()
	if s.Hooks != 2 || s.HooksDropped != 18 {
		t.Fatalf("hooks=%d dropped=%d, want 2 and 18", s.Hooks, s.HooksDropped)
	}
	if len(p.readAccounts()) != 20 {
		t.Fatal("dropping hooks must never drop data")
	}
}

// A hook that panics is contained: replication goes on.
func TestChaos_PanickingHookIsContained(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-panic")
	svc := newService(t, p, f, func(c *cqrsSvc) {
		c.ToResource = func(*Account) *AccountResponse { panic("boom") }
		c.Created = func(*Account) []string { return []string{"c"} }
	})
	start(t, svc)
	if err := f.deliver(ctxTimeout(t, time.Minute), seq(0, 0, 1, 5)); err != nil {
		t.Fatal(err)
	}
	if err := f.deliver(ctxTimeout(t, time.Minute), seq(0, 5, 6, 5)); err != nil {
		t.Fatal(err)
	}
	if len(p.readAccounts()) != 10 {
		t.Fatal("a panicking hook must not stop replication")
	}
}

// ---- database failover & primary-replica promotion during a transaction ----

// The read database turns read-only (a replica before promotion): the batch is
// retried, never skipped and never half-applied. When it is promoted (writable
// again) the same batch commits.
func TestFailover_ReadOnlyThenPromoted(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-failover")
	svc := newService(t, p, f)
	start(t, svc)
	ctx := ctxTimeout(t, time.Minute)
	if err := f.deliver(ctx, seq(0, 0, 1, 5)); err != nil {
		t.Fatal(err)
	}

	setReadOnly(t, p, true)
	killReadBackends(t, p, 0) // established sessions go away, as at a failover
	done := async(func() error { return f.deliver(ctx, seq(0, 5, 6, 5)) })
	if !stays(done, 700*time.Millisecond) {
		t.Fatal("a read-only database must stall the batch, not drop it")
	}
	if len(p.readAccounts()) != 5 {
		t.Fatal("nothing of the stalled batch may be visible")
	}
	if !strings.Contains(svc.Stats().LastError, "25006") && !strings.Contains(svc.Stats().LastError, "read-only") {
		t.Logf("last error: %s", svc.Stats().LastError)
	}
	// Promotion. default_transaction_read_only is only read when a session starts,
	// so the connections the pool opened while it was on would stay read-only: a
	// real promotion makes live sessions writable, so reset them to match.
	setReadOnly(t, p, false)
	killReadBackends(t, p, 0)
	if err := within(t, done, 20*time.Second, "the batch after promotion (last error: "+svc.Stats().LastError+")"); err != nil {
		t.Fatal(err)
	}
	if len(p.readAccounts()) != 10 || p.checkpoint("g-failover", 0) != 10 {
		t.Fatal("the batch must commit completely after promotion")
	}
}

// The connection dies in the middle of the transaction: the rows and the
// checkpoint commit together or not at all, and the retry applies the batch once.
func TestFailover_ConnectionKilledMidTransaction(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-kill")
	svc := newService(t, p, f)
	start(t, svc)
	ctx := ctxTimeout(t, time.Minute)
	if err := f.deliver(ctx, seq(0, 0, 1, 3)); err != nil {
		t.Fatal(err)
	}
	// Block the batch on the checkpoint row, then kill the service's connections
	// while it is inside its transaction.
	holder, release := holdRead(t, p, `SELECT * FROM cqrs_checkpoints FOR UPDATE`)
	done := async(func() error { return f.deliver(ctx, seq(0, 3, 4, 20)) })
	time.Sleep(400 * time.Millisecond)
	killReadBackends(t, p, holder)
	time.Sleep(200 * time.Millisecond)
	release()
	if err := within(t, done, 30*time.Second, "the batch after the kill"); err != nil {
		t.Fatal(err)
	}
	if len(p.readAccounts()) != 23 || p.checkpoint("g-kill", 0) != 23 {
		t.Fatalf("rows=%d checkpoint=%d", len(p.readAccounts()), p.checkpoint("g-kill", 0))
	}
}

// ---- database connection pool exhaustion & timeout handling ----------------

// Every connection of the pool is taken by someone else: the batch waits (it
// cannot get a connection), then proceeds the moment one is free. It neither
// fails permanently nor deadlocks.
func TestPool_ExhaustionStallsThenRecovers(t *testing.T) {
	t.Parallel()
	p := newPair(t, 1) // a pool of exactly one connection
	f := newFakeCDC("g-pool")
	svc := newService(t, p, f, func(c *cqrsSvc) { c.ApplyTimeout = 800 * time.Millisecond })
	start(t, svc)

	hold, err := p.read.Client().Conn(context.Background()) // take the only connection
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxTimeout(t, time.Minute)
	done := async(func() error { return f.deliver(ctx, seq(0, 0, 1, 5)) })
	if !stays(done, 1500*time.Millisecond) {
		t.Fatal("a batch with no connection available must wait")
	}
	if svc.Stats().Retries == 0 {
		t.Fatal("the timeouts must be retried, and visible")
	}
	if len(f.deadLettered()) != 0 {
		t.Fatal("a timeout is transient: it must never dead-letter data")
	}
	_ = hold.Close()
	if err := within(t, done, 20*time.Second, "the batch after the connection came back"); err != nil {
		t.Fatal(err)
	}
	if len(p.readAccounts()) != 5 {
		t.Fatal("the batch must apply once a connection is free")
	}
}

// ---- partition re-assignment and offset commit failure ---------------------

// Kafka's offset commit fails (the consumer lost the partition): the service
// carries on, counts it, and the redelivery is skipped by the checkpoint.
func TestRebalance_CommitFailureThenRedeliveryIsSkipped(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-commit")
	svc := newService(t, p, f)
	start(t, svc)
	ctx := ctxTimeout(t, time.Minute)
	batch := seq(0, 0, 1, 10)
	if err := f.deliver(ctx, batch); err != nil {
		t.Fatal(err)
	}
	f.commitError(0, 9, errors.New("rebalance in progress"))
	if svc.Stats().CommitFails != 1 {
		t.Fatalf("commit failures = %d", svc.Stats().CommitFails)
	}
	// Kafka never saw the commit, so the same records come again, from a new owner.
	if err := f.deliver(ctx, batch); err != nil {
		t.Fatal(err)
	}
	s := svc.Stats()
	if s.Applied != 10 || s.Duplicates != 10 {
		t.Fatalf("applied=%d duplicates=%d, want 10 and 10", s.Applied, s.Duplicates)
	}
}

// A partition moves from one replica to another in the middle of the stream:
// the new owner continues from the checkpoint, whatever Kafka redelivers.
func TestRebalance_PartitionMovesBetweenReplicas(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	const group = "g-move"
	a, b := newFakeCDC(group), newFakeCDC(group)
	sa, sb := newService(t, p, a), newService(t, p, b)
	start(t, sa)
	start(t, sb)
	ctx := ctxTimeout(t, time.Minute)
	stream := seq(0, 0, 1, 60)

	if err := a.deliver(ctx, stream[:30]); err != nil {
		t.Fatal(err)
	}
	a.rebalance([]int32{0}, nil)
	b.rebalance(nil, []int32{0})
	// Kafka had only committed part of what A applied, so B is handed an older start.
	if err := b.deliver(ctx, stream[10:50]); err != nil {
		t.Fatal(err)
	}
	if err := b.deliver(ctx, stream[50:]); err != nil {
		t.Fatal(err)
	}
	if n := p.count(p.read, "accounts"); n != 60 {
		t.Fatalf("rows = %d, want 60", n)
	}
	if next := p.checkpoint(group, 0); next != 60 {
		t.Fatalf("checkpoint = %d", next)
	}
	if got := sb.Stats().Applied; got != 30 {
		t.Fatalf("B wrote %d rows, want only the 30 A had not written", got)
	}
}

// A topic that was deleted and recreated starts again at offset 0 with newer
// data. Skipping it as "already applied" would silently lose everything, so
// replication pauses (loudly) instead.
func TestRebalance_RecreatedTopicPausesInsteadOfSkipping(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-recreate")
	svc := newService(t, p, f)
	start(t, svc)
	ctx := ctxTimeout(t, time.Minute)
	old := baseTime
	var first []cdc.Record
	for i := range 5 {
		first = append(first, create(0, int64(i), int64(i+1), "old", 1, 1, at(old)))
	}
	if err := f.deliver(ctx, first); err != nil {
		t.Fatal(err)
	}
	fresh := []cdc.Record{create(0, 0, 100, "recreated", 1, 1, at(old.Add(48*time.Hour)))}
	done := async(func() error { return f.deliver(ctx, fresh) })
	if !stays(done, 600*time.Millisecond) {
		t.Fatal("a recreated topic must pause replication, not be skipped")
	}
	if !strings.Contains(svc.Stats().LastError, "recreated") {
		t.Fatalf("the reason must be in the stats: %q", svc.Stats().LastError)
	}
	if _, ok := p.readAccounts()[100]; ok {
		t.Fatal("nothing may be applied while paused")
	}
	p.exec(p.read, `DELETE FROM cqrs_checkpoints WHERE topic = ?`, testTopic) // the documented remedy
	if err := within(t, done, 20*time.Second, "the batch after the checkpoint was cleared"); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.readAccounts()[100]; !ok {
		t.Fatal("the recreated topic must apply once the checkpoint is cleared")
	}
	_ = cqrs.ErrOffsetRegression
}

// ---- high volume, pool hygiene, large payloads -----------------------------

// A large stream: everything arrives, the pooled scratch space never carries
// rows from one batch into the next, and memory stays bounded.
func TestVolume_LargeStreamAndPoolHygiene(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-volume")
	svc := newService(t, p, f, func(c *cqrsSvc) { c.BatchSize = 500 })
	start(t, svc)
	ctx := ctxTimeout(t, 4*time.Minute)

	const total, per = 20000, 500
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for off := 0; off < total; off += per {
		var batch []cdc.Record
		for i := range per {
			n := int64(off + i)
			batch = append(batch, create(0, n, n+1, fmt.Sprintf("owner-%d", n), n, 1))
		}
		if err := f.deliver(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	if n := p.count(p.read, "accounts"); n != total {
		t.Fatalf("rows = %d, want %d", n, total)
	}
	got := p.readAccounts()
	for _, id := range []int64{1, 777, 10000, total} {
		if want := fmt.Sprintf("owner-%d", id-1); got[id].Owner != want {
			t.Fatalf("row %d owner = %q, want %q", id, got[id].Owner, want)
		}
	}
	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("heap growth over %d rows: %d KiB", total, growth/1024)
	if growth > 200<<20 {
		t.Fatalf("heap grew by %d MiB; the service is holding on to batches", growth>>20)
	}
}

// Rows with very large text are applied exactly and do not blow up memory.
func TestVolume_LargePayloads(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-large")
	start(t, newService(t, p, f))
	big := strings.Repeat("x", 4<<20) // 4 MiB per row
	var batch []cdc.Record
	for i := range 6 {
		batch = append(batch, event(0, int64(i), cdc.OpCreate, int64(i+1), "null",
			fmt.Sprintf(`{"id":%d,"owner":%q,"balance":1,"version":1}`, i+1, big)))
	}
	if err := f.deliver(ctxTimeout(t, 3*time.Minute), batch); err != nil {
		t.Fatal(err)
	}
	for id, a := range p.readAccounts() {
		if len(a.Owner) != len(big) {
			t.Fatalf("row %d owner has %d bytes, want %d", id, len(a.Owner), len(big))
		}
	}
	if p.count(p.read, "accounts") != 6 {
		t.Fatal("all large rows must arrive")
	}
}

// Many goroutines applying at once: nothing is shared between batches, so the
// race detector stays quiet and every row lands.
func TestVolume_ConcurrentBatchesShareNothing(t *testing.T) {
	t.Parallel()
	p := newPair(t, 16)
	const workers, per = 12, 100
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	ctx := ctxTimeout(t, 3*time.Minute)
	for w := range workers {
		f := newFakeCDC("g-conc")
		start(t, newService(t, p, f))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for off := 0; off < per; off += 20 {
				var batch []cdc.Record
				for i := range 20 {
					n := int64(off + i)
					batch = append(batch, create(int32(w), n, int64(w*10000)+n+1, "o", 1, 1))
				}
				if err := f.deliver(ctx, batch); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if n := p.count(p.read, "accounts"); n != workers*per {
		t.Fatalf("rows = %d, want %d", n, workers*per)
	}
}
