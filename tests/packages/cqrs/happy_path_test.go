//go:build integration

package cqrs_test

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// Creates, updates and deletes reach the read database, the checkpoint moves to
// the last offset, and Kafka is told about it.
func TestHappy_CreateUpdateDelete(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-happy")
	svc := newService(t, p, f)
	start(t, svc)
	ctx := ctxTimeout(t, time.Minute)

	batch := []cdc.Record{
		create(0, 0, 1, "ana", 100, 1),
		create(0, 1, 2, "ben", 200, 1),
		update(0, 2, 1, "ana", 150, 2),
		remove(0, 3, 2),
	}
	if err := f.deliver(ctx, batch); err != nil {
		t.Fatal(err)
	}
	got := p.readAccounts()
	if len(got) != 1 || got[1].Balance != 150 || got[1].Version != 2 || got[1].Owner != "ana" {
		t.Fatalf("read db = %+v", got)
	}
	if next := p.checkpoint("g-happy", 0); next != 4 {
		t.Fatalf("checkpoint = %d, want 4", next)
	}
	if c := f.committedOffset(0); c != 4 {
		t.Fatalf("kafka commit = %d, want 4", c)
	}
	if s := svc.Stats(); s.Batches != 1 || s.Records != 4 || s.Applied != 2 {
		t.Fatalf("stats = %+v", s)
	}
}

// A change hook fires once per committed change, after the data is durable.
func TestHappy_HooksFireAfterCommit(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-hooks")
	rec := &recorder{}
	svc := newService(t, p, f, func(c *cqrsSvc) {
		c.ToResource = func(a *Account) *AccountResponse { return &AccountResponse{ID: a.ID, Owner: a.Owner} }
		c.Created = func(*Account) []string { return []string{"created"} }
		c.Updated = func(*Account) []string { return []string{"updated"} }
		c.Deleted = func(*Account) []string { return []string{"deleted"} }
		c.Dispatch = rec.dispatch
	})
	start(t, svc)
	if err := f.deliver(ctxTimeout(t, time.Minute), []cdc.Record{
		create(0, 0, 1, "a", 1, 1), update(0, 1, 1, "b", 2, 2), remove(0, 2, 1),
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "3 hook events", func() bool { return rec.count() == 3 })
	if len(p.readAccounts()) != 0 {
		t.Fatal("the row was deleted")
	}
}

// Several changes to one row inside a batch collapse to its final state, and
// rows in other partitions are independent.
func TestHappy_BatchCollapsesAndPartitionsAreIndependent(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-collapse")
	start(t, newService(t, p, f))
	ctx := ctxTimeout(t, time.Minute)
	if err := f.deliver(ctx, []cdc.Record{
		create(0, 0, 5, "v1", 1, 1), update(0, 1, 5, "v2", 2, 2), update(0, 2, 5, "v3", 3, 3),
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.deliver(ctx, []cdc.Record{create(1, 0, 6, "other", 9, 1)}); err != nil {
		t.Fatal(err)
	}
	got := p.readAccounts()
	if got[5].Owner != "v3" || got[5].Balance != 3 || got[6].Owner != "other" {
		t.Fatalf("read db = %+v", got)
	}
	if p.checkpoint("g-collapse", 0) != 3 || p.checkpoint("g-collapse", 1) != 1 {
		t.Fatal("each partition has its own checkpoint")
	}
}

// Every column type survives the trip exactly, including values bun would turn
// into DEFAULT on a model insert (zero, nil).
func TestHappy_ExactValues(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-exact")
	start(t, newService(t, p, f))
	row := `{"id":3,"owner":"","balance":0,"note":null,"active":false,"updated_at":"2026-05-06T07:08:09.654321Z","version":0}`
	if err := f.deliver(ctxTimeout(t, time.Minute), []cdc.Record{event(0, 0, cdc.OpCreate, 3, "null", row)}); err != nil {
		t.Fatal(err)
	}
	// A column whose default differs from the zero value must still be written as zero.
	p.exec(p.read, `ALTER TABLE accounts ALTER COLUMN owner SET DEFAULT 'DEFAULT-OWNER', ALTER COLUMN balance SET DEFAULT 77`)
	if err := f.deliver(ctxTimeout(t, time.Minute), []cdc.Record{event(0, 1, cdc.OpCreate, 4, "null", row[:6]+"4"+row[7:])}); err != nil {
		t.Fatal(err)
	}
	got := p.readAccounts()
	for _, id := range []int64{3, 4} {
		a := got[id]
		if a.Owner != "" || a.Balance != 0 || a.Note != nil || a.Active {
			t.Fatalf("row %d is not an exact copy: %+v", id, a)
		}
		if want := time.Date(2026, 5, 6, 7, 8, 9, 654321000, time.UTC); !a.UpdatedAt.Equal(want) {
			t.Fatalf("row %d updated_at = %v", id, a.UpdatedAt)
		}
	}
}

// Run twice is a no-op; Stop twice is a no-op; Run again after Stop works.
func TestHappy_LifecycleIsIdempotent(t *testing.T) {
	t.Parallel()
	p := newPair(t, 8)
	f := newFakeCDC("g-life")
	svc := newService(t, p, f)
	ctx := context.Background()
	for range 2 {
		if err := svc.Run(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !f.subscribed() {
		t.Fatal("not subscribed after Run")
	}
	for range 2 {
		if err := svc.Stop(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if f.subscribed() {
		t.Fatal("still subscribed after Stop")
	}
	if err := svc.Run(ctx); err != nil {
		t.Fatalf("Run after Stop: %v", err)
	}
	if err := svc.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}
