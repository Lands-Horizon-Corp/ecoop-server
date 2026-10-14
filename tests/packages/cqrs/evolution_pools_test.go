//go:build integration

package cqrs_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// ---- schema evolution & backward compatibility -----------------------------

// Newer is Account plus a column the older events do not have.
type Newer struct {
	bun.BaseModel `bun:"table:accounts"`

	ID        int64     `bun:"id,pk"`
	Owner     string    `bun:"owner"`
	Balance   int64     `bun:"balance"`
	Note      *string   `bun:"note"`
	Active    bool      `bun:"active"`
	UpdatedAt time.Time `bun:"updated_at,type:timestamptz"`
	Version   int64     `bun:"version"`
	Nickname  string    `bun:"nickname"`
	Tier      *int64    `bun:"tier"`
}

// A service built for a newer schema applies events from an older producer: the
// columns the events lack take their zero value, and nothing is rejected.
func TestEvolution_NewServiceOldEvents(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	p.exec(p.read, `ALTER TABLE accounts ADD COLUMN nickname text NOT NULL DEFAULT 'db-default', ADD COLUMN tier bigint`)
	f := newFakeCDC("g-evo-new")
	svc := newTyped[Newer](t, p, f, "version")
	start(t, svc)
	ctx := ctxTimeout(t, time.Minute)
	if err := f.deliver(ctx, []cdc.Record{create(0, 0, 1, "old-event", 5, 1)}); err != nil {
		t.Fatal(err)
	}
	var got Newer
	if err := p.read.Client().NewSelect().Model(&got).Where("id = 1").Scan(ctx); err != nil {
		t.Fatal(err)
	}
	if got.Owner != "old-event" || got.Nickname != "" || got.Tier != nil {
		t.Fatalf("columns missing from the event must be zero, not the database default: %+v", got)
	}
	// An event that does carry the new columns fills them.
	full := event(0, 1, cdc.OpUpdate, 1, "null", `{"id":1,"owner":"new-event","balance":6,"version":2,"nickname":"nick","tier":3}`)
	if err := f.deliver(ctx, []cdc.Record{full}); err != nil {
		t.Fatal(err)
	}
	got = Newer{}
	if err := p.read.Client().NewSelect().Model(&got).Where("id = 1").Scan(ctx); err != nil {
		t.Fatal(err)
	}
	if got.Nickname != "nick" || got.Tier == nil || *got.Tier != 3 {
		t.Fatalf("new columns = %+v", got)
	}
}

// A service built for an older schema applies events from a newer producer: the
// columns it does not know are ignored.
func TestEvolution_OldServiceNewEvents(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-evo-old")
	svc := newService(t, p, f)
	start(t, svc)
	ev := event(0, 0, cdc.OpCreate, 1, "null",
		`{"id":1,"owner":"o","balance":2,"version":1,"nickname":"unknown","tier":9,"deeply":{"nested":[1,2,3]}}`)
	if err := f.deliver(ctxTimeout(t, time.Minute), []cdc.Record{ev}); err != nil {
		t.Fatal(err)
	}
	if got := p.readAccounts()[1]; got.Owner != "o" || got.Balance != 2 {
		t.Fatalf("row = %+v", got)
	}
	if svc.Stats().DeadLettered != 0 {
		t.Fatal("unknown columns are not poison")
	}
}

// The read database is migrated after the app that needs a column starts: Run
// refuses to start, says which column is missing, and works once it exists.
func TestEvolution_ReadDatabaseBehindTheModel(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-evo-behind")
	svc := newTyped[Newer](t, p, f, "version")
	err := svc.Run(context.Background())
	if err == nil || !contains(err.Error(), "nickname") || !contains(err.Error(), "tier") {
		t.Fatalf("Run must name the missing columns: %v", err)
	}
	p.exec(p.read, `ALTER TABLE accounts ADD COLUMN nickname text NOT NULL DEFAULT '', ADD COLUMN tier bigint`)
	start(t, svc)
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// A column the READ database dropped while the model still maps it fails on the
// first write: that is permanent for the data, so it must stall (a migration is
// the cure) rather than dead-letter every change.
func TestEvolution_ColumnDroppedUnderneathStalls(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-evo-drop")
	svc := newService(t, p, f)
	start(t, svc)
	p.exec(p.read, `ALTER TABLE accounts DROP COLUMN note`)
	ctx := ctxTimeout(t, time.Minute)
	done := async(func() error { return f.deliver(ctx, []cdc.Record{create(0, 0, 1, "x", 1, 1)}) })
	if !stays(done, 800*time.Millisecond) {
		t.Fatal("a missing column is a migration problem: the batch must wait, not be skipped")
	}
	if len(f.deadLettered()) != 0 {
		t.Fatal("nothing may be dead-lettered because of a schema mismatch")
	}
	p.exec(p.read, `ALTER TABLE accounts ADD COLUMN note text`)
	if err := within(t, done, 20*time.Second, "the batch after the column came back"); err != nil {
		t.Fatal(err)
	}
	if len(p.readAccounts()) != 1 {
		t.Fatal("the row must arrive once the schema is right")
	}
}

// ---- memory pool exhaustion, leaks and reuse -------------------------------

// Far more distinct keys than the pools keep (their caps are 10000) in one batch,
// then ordinary small batches: oversized scratch space is dropped rather than
// kept, and a later batch never sees what an earlier one held.
func TestPools_OversizedBatchThenSmallOnes(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-pools")
	svc := newService(t, p, f, func(c *cqrsSvc) { c.BatchSize = 12000 })
	start(t, svc)
	ctx := ctxTimeout(t, 3*time.Minute)

	var big []cdc.Record
	for i := range 12000 {
		big = append(big, create(0, int64(i), int64(i+1), "big", 1, 1))
	}
	if err := f.deliver(ctx, big); err != nil {
		t.Fatal(err)
	}
	for round := range 20 {
		off := int64(12000 + round*3)
		small := []cdc.Record{
			update(0, off, 1, fmt.Sprintf("r%d", round), int64(round), int64(round+2)),
			create(0, off+1, int64(100000+round*2), "n1", 1, 1),
			create(0, off+2, int64(100001+round*2), "n2", 1, 1),
		}
		if err := f.deliver(ctx, small); err != nil {
			t.Fatal(err)
		}
	}
	if n := p.count(p.read, "accounts"); n != 12000+40 {
		t.Fatalf("rows = %d, want %d", n, 12040)
	}
	if got := p.readAccounts()[1]; got.Owner != "r19" {
		t.Fatalf("row 1 owner = %q, want r19", got.Owner)
	}
}

// Many concurrent services acquiring and returning pooled scratch space while
// the garbage collector runs: no corruption (every row exact), and the heap
// settles afterwards, so nothing is held on to.
func TestPools_ConcurrentAcquireReleaseDoesNotLeak(t *testing.T) {
	t.Parallel()
	p := newPair(t, 16)
	const workers, rounds = 10, 60
	ctx := ctxTimeout(t, 4*time.Minute)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		f := newFakeCDC("g-poolrace")
		start(t, newService(t, p, f))
		wg.Add(1)
		go func() {
			defer wg.Done()
			off := int64(0)
			for r := range rounds {
				var batch []cdc.Record
				for i := range 30 {
					id := int64(w*100000 + (r*30+i)%200 + 1) // reuse keys: collapse and upsert paths
					batch = append(batch, update(int32(w), off, id, fmt.Sprintf("w%d-r%d-i%d", w, r, i), id, off+1))
					off++
				}
				if err := f.deliver(ctx, batch); err != nil {
					errs <- err
					return
				}
				if r%10 == 0 {
					runtime.GC()
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	for w := range workers {
		// The last write to each key is the one that must have won.
		rows := map[int64]Account{}
		for id, a := range p.readAccounts() {
			if id/100000 == int64(w) {
				rows[id] = a
			}
		}
		if len(rows) != 200 {
			t.Fatalf("worker %d: %d rows, want 200", w, len(rows))
		}
		for id, a := range rows {
			if a.Balance != id {
				t.Fatalf("worker %d row %d balance = %d (rows must not mix between batches)", w, id, a.Balance)
			}
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if growth := int64(after.HeapInuse) - int64(before.HeapInuse); growth > 150<<20 {
		t.Fatalf("heap in use grew by %d MiB across %d batches", growth>>20, workers*rounds)
	}
	_ = cqrs.CheckpointTable
}
