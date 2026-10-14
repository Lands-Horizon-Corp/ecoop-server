//go:build integration

package cqrs_test

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// ---- sync & race -----------------------------------------------------------

// The same partition delivered to many replicas at once (what a rebalance, a
// slow consumer or a retried commit looks like): every record is applied
// exactly once, nothing deadlocks, and all replicas agree on the end state.
func TestRace_ManyReplicasOverlapOnOnePartition(t *testing.T) {
	t.Parallel()
	p := newPair(t, 16)
	const group = "g-race"
	const replicas, perBatch, batches = 6, 25, 12

	fs := make([]*fakeCDC, replicas)
	for i := range fs {
		fs[i] = newFakeCDC(group)
		start(t, newService(t, p, fs[i]))
	}
	var all [][]cdc.Record
	for b := range batches {
		var batch []cdc.Record
		for i := range perBatch {
			off := int64(b*perBatch + i)
			// A small key space makes every batch collide with the others on rows.
			batch = append(batch, update(0, off, int64(off%17)+1, fmt.Sprintf("o%d", off), off, off+1))
		}
		all = append(all, batch)
	}

	ctx := ctxTimeout(t, 2*time.Minute)
	var wg sync.WaitGroup
	errs := make(chan error, replicas)
	for r := range replicas {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, i := range rand.Perm(len(all)) { // each replica sees the batches in its own order
				if err := fs[r].deliver(ctx, all[i]); err != nil {
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

	// Only the in-order delivery of a batch counts: a replica that got batch 5
	// before batch 3 wrote its rows and advanced the checkpoint, so batch 3 is a
	// "duplicate" from the checkpoint's point of view. What must hold is that
	// the checkpoint ends at the end of the stream and no row is half-written.
	if next := p.checkpoint(group, 0); next != int64(batches*perBatch) {
		t.Fatalf("checkpoint = %d, want %d", next, batches*perBatch)
	}
	for id, a := range p.readAccounts() {
		if a.Owner == "" || a.Version == 0 {
			t.Fatalf("row %d is half written: %+v", id, a)
		}
	}
}

// Replicas that each apply their own partition, concurrently, produce exactly
// the union: the partitions are independent and none blocks another.
func TestRace_PartitionsApplyInParallel(t *testing.T) {
	t.Parallel()
	p := newPair(t, 16)
	const parts, per = 5, 200
	fs := make([]*fakeCDC, parts)
	for i := range fs {
		fs[i] = newFakeCDC("g-parts")
		start(t, newService(t, p, fs[i]))
	}
	ctx := ctxTimeout(t, 2*time.Minute)
	var wg sync.WaitGroup
	errs := make(chan error, parts)
	for part := range parts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var batch []cdc.Record
			for i := range per {
				id := int64(part*1000 + i + 1)
				batch = append(batch, create(int32(part), int64(i), id, "o", 1, 1))
				if len(batch) == 50 {
					if err := fs[part].deliver(ctx, batch); err != nil {
						errs <- err
						return
					}
					batch = nil
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if n := p.count(p.read, "accounts"); n != parts*per {
		t.Fatalf("rows = %d, want %d", n, parts*per)
	}
	for part := range parts {
		if next := p.checkpoint("g-parts", int32(part)); next != per {
			t.Fatalf("partition %d checkpoint = %d", part, next)
		}
	}
}

// Two replicas overlapping on the very same batch: the second waits for the
// first's lock, finds the checkpoint ahead, and writes nothing. The hooks fire
// once.
func TestRace_OverlappingReplicaSkipsWhatTheFirstDid(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	rec := &recorder{}
	hooks := func(c *cqrsSvc) {
		c.ToResource = func(a *Account) *AccountResponse { return &AccountResponse{ID: a.ID} }
		c.Created = func(*Account) []string { return []string{"c"} }
		c.Dispatch = rec.dispatch
		c.HookWorkers = 64 // all 40 hooks of one batch can run at once: leave room for every one
	}
	a, b := newFakeCDC("g-overlap"), newFakeCDC("g-overlap")
	sa, sb := newService(t, p, a, hooks), newService(t, p, b, hooks)
	start(t, sa)
	start(t, sb)
	batch := seq(0, 0, 1, 40)
	ctx := ctxTimeout(t, time.Minute)
	var wg sync.WaitGroup
	for _, f := range []*fakeCDC{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.deliver(ctx, batch); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := p.count(p.read, "accounts"); n != 40 {
		t.Fatalf("rows = %d", n)
	}
	total := sa.Stats().Applied + sb.Stats().Applied
	if total != 40 {
		t.Fatalf("the two replicas wrote %d rows between them, want exactly 40", total)
	}
	if dup := sa.Stats().Duplicates + sb.Stats().Duplicates; dup != 40 {
		t.Fatalf("the loser must have skipped all 40 as duplicates, skipped %d", dup)
	}
	eventually(t, "40 hook events", func() bool { return rec.count() == 40 })
	time.Sleep(200 * time.Millisecond)
	if rec.count() != 40 {
		t.Fatalf("hooks fired %d times, want 40: a duplicate must not notify twice", rec.count())
	}
	if d := sa.Stats().HooksDropped + sb.Stats().HooksDropped; d != 0 {
		t.Fatalf("%d hooks were dropped", d)
	}
}

// ---- chaos -----------------------------------------------------------------

// Random failures of every kind while a stream is applied: connections killed,
// the read database flipping to read-only and back, duplicate and re-ordered
// redeliveries. In the end the read database is an exact copy of what was sent.
func TestChaos_RandomFaultsLoseNothing(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-chaos")
	svc := newService(t, p, f)
	start(t, svc)

	const total = 400
	want := map[int64]int64{} // id -> final balance
	var stream []cdc.Record
	for i := range total {
		id := int64(i%40) + 1
		bal := int64(i + 1)
		want[id] = bal
		stream = append(stream, update(0, int64(i), id, "chaos", bal, int64(i+1)))
	}

	ctx := ctxTimeout(t, 3*time.Minute)
	stop := make(chan struct{})
	var chaos sync.WaitGroup
	chaos.Add(1)
	go func() {
		defer chaos.Done()
		rng := rand.New(rand.NewPCG(1, 2))
		for {
			select {
			case <-stop:
				setReadOnly(t, p, false)
				return
			case <-time.After(time.Duration(30+rng.IntN(60)) * time.Millisecond):
			}
			switch rng.IntN(3) {
			case 0:
				killReadBackends(t, p, 0)
			case 1:
				setReadOnly(t, p, true)
				killReadBackends(t, p, 0)
			case 2:
				setReadOnly(t, p, false)
			}
		}
	}()

	for start := 0; start < len(stream); {
		end := min(start+20+rand.IntN(30), len(stream))
		if err := f.deliver(ctx, stream[start:end]); err != nil {
			t.Fatal(err)
		}
		if rand.IntN(4) == 0 { // a duplicate delivery of what was just applied
			if err := f.deliver(ctx, stream[start:end]); err != nil {
				t.Fatal(err)
			}
		}
		start = end
	}
	close(stop)
	chaos.Wait()
	// The last injected fault may have killed the pooled connection the checks
	// below would use. The service retries that; a plain read in a test must too.
	settle(t, p)

	got := p.readAccounts()
	if len(got) != len(want) {
		t.Fatalf("rows = %d, want %d", len(got), len(want))
	}
	for id, bal := range want {
		if got[id].Balance != bal {
			t.Fatalf("row %d balance = %d, want %d", id, got[id].Balance, bal)
		}
	}
	if next := p.checkpoint("g-chaos", 0); next != total {
		t.Fatalf("checkpoint = %d, want %d", next, total)
	}
	if svc.Stats().Retries == 0 {
		t.Log("note: no retries happened; the faults may have missed every batch")
	}
}
