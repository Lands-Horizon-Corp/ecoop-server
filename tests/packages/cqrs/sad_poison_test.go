//go:build integration

package cqrs_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// ---- sad path --------------------------------------------------------------

// Run refuses to start half-ready, and says exactly what is missing.
func TestSad_RunRefusesWhatIsNotReady(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	check := func(name string, want error, prepare func(p *pair)) {
		t.Run(name, func(t *testing.T) {
			p := newPair(t, 4)
			f := newFakeCDC("g-sad")
			svc := newService(t, p, f)
			prepare(p)
			if err := svc.Run(ctx); !errors.Is(err, want) {
				t.Fatalf("Run = %v, want %v", err, want)
			}
			if f.subscribed() {
				t.Fatal("a refused Run must not subscribe")
			}
			if err := svc.Stop(ctx); err != nil {
				t.Fatalf("Stop after a refused Run: %v", err)
			}
		})
	}
	check("read table missing", cqrs.ErrReadTableMissing, func(p *pair) { p.exec(p.read, `DROP TABLE accounts`) })
	check("read column missing", cqrs.ErrReadTableMissing, func(p *pair) { p.exec(p.read, `ALTER TABLE accounts DROP COLUMN note`) })
	check("checkpoint table missing", cqrs.ErrReadTableMissing, func(p *pair) { p.exec(p.read, `DROP TABLE cqrs_checkpoints`) })
	check("write table missing", cqrs.ErrWriteTableMissing, func(p *pair) { p.exec(p.write, `DROP TABLE accounts`) })
	check("replica identity not full", cqrs.ErrReplicaIdentity, func(p *pair) {
		p.exec(p.write, `ALTER TABLE accounts REPLICA IDENTITY DEFAULT`)
	})
	check("read db stopped", cqrs.ErrReadDBNotInitialized, func(p *pair) { _ = p.read.Stop(ctx) })
	check("write db stopped", cqrs.ErrWriteDBNotInitialized, func(p *pair) { _ = p.write.Stop(ctx) })
}

// A failed subscription leaves the service stopped and able to start again.
func TestSad_SubscribeFailureRollsBack(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-sub")
	svc := newService(t, p, f)
	noop := func(context.Context, []cdc.Record) error { return nil }
	if err := f.SubscribeBatch(context.Background(), "other", cdc.BatchOptions{}, noop); err != nil { // occupy the slot
		t.Fatal(err)
	}
	if err := svc.Run(context.Background()); !errors.Is(err, cdc.ErrDuplicateSubscription) {
		t.Fatalf("Run = %v", err)
	}
	_ = f.Unsubscribe(context.Background(), "other")
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run after the slot was freed: %v", err)
	}
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// A malformed batch (the contract cdc promises is broken) is refused at once,
// not retried forever and never written.
func TestSad_MalformedBatchIsRefused(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-bad")
	start(t, newService(t, p, f))
	bad := []cdc.Record{create(0, 5, 1, "a", 1, 1), create(0, 4, 2, "b", 1, 1)}
	if err := f.deliverOnce(ctxTimeout(t, 10*time.Second), bad); !errors.Is(err, cqrs.ErrBadBatch) {
		t.Fatalf("got %v", err)
	}
	if n := p.count(p.read, "accounts"); n != 0 {
		t.Fatalf("a refused batch wrote %d rows", n)
	}
	if err := f.deliverOnce(ctxTimeout(t, 10*time.Second), nil); err != nil {
		t.Fatalf("an empty batch is a no-op: %v", err)
	}
}

// A conflict the data itself cannot resolve in this batch (a unique index the
// read db has and the write db does not) is never skipped: replication waits.
// Once the cause is fixed the same change applies.
func TestSad_NeverSkipsWhatMayStillSucceed(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-wait")
	svc := newService(t, p, f)
	start(t, svc)
	p.exec(p.read, `CREATE UNIQUE INDEX accounts_owner_uq ON accounts (owner)`)
	ctx := ctxTimeout(t, time.Minute)
	if err := f.deliver(ctx, []cdc.Record{create(0, 0, 1, "same", 1, 1)}); err != nil {
		t.Fatal(err)
	}
	done := async(func() error { return f.deliver(ctx, []cdc.Record{create(0, 1, 2, "same", 1, 1)}) })
	if !stays(done, 600*time.Millisecond) {
		t.Fatal("a unique violation must stall the partition, not drop the change")
	}
	if len(f.deadLettered()) != 0 {
		t.Fatal("a change that may still succeed must not be dead-lettered")
	}
	if svc.Stats().Retries == 0 {
		t.Fatal("the stall must be visible in the stats")
	}
	p.exec(p.read, `DROP INDEX accounts_owner_uq`)
	if err := within(t, done, 20*time.Second, "the stalled batch"); err != nil {
		t.Fatal(err)
	}
	if got := p.readAccounts(); len(got) != 2 {
		t.Fatalf("both rows must be there once the cause is fixed: %+v", got)
	}
}

// ---- poison pill -----------------------------------------------------------

// A record that cannot be decoded is parked once on the dead-letter topic; the
// records around it are applied, in the same batch, and the checkpoint passes it.
func TestPoison_UndecodableRecordIsParkedOnce(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-poison")
	svc := newService(t, p, f)
	start(t, svc)
	ctx := ctxTimeout(t, time.Minute)
	batch := []cdc.Record{create(0, 0, 1, "ok", 1, 1), poison(0, 1), create(0, 2, 3, "ok", 1, 1)}
	if err := f.deliver(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if got := p.readAccounts(); len(got) != 2 {
		t.Fatalf("the neighbours of a poison record must apply: %+v", got)
	}
	if d := f.deadLettered(); len(d) != 1 || d[0].Offset != 1 {
		t.Fatalf("dead letters = %+v", d)
	}
	if p.checkpoint("g-poison", 0) != 3 {
		t.Fatal("the checkpoint must pass the poison record")
	}
	// Delivered again (a crash before the Kafka commit): not parked twice.
	if err := f.deliver(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if d := f.deadLettered(); len(d) != 1 {
		t.Fatalf("a redelivered poison record was parked again: %d", len(d))
	}
	if s := svc.Stats(); s.DeadLettered != 1 || s.Duplicates != 3 {
		t.Fatalf("stats = %+v", s)
	}
}

// Rows the read database rejects for good (a CHECK the write db lacks, a NUL
// byte) are found by splitting the batch: only they are parked, in order.
func TestPoison_RejectedRowsAreIsolatedByBisection(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-bisect")
	svc := newService(t, p, f)
	start(t, svc)
	p.exec(p.read, `ALTER TABLE accounts ADD CONSTRAINT balance_ok CHECK (balance >= 0)`)

	var batch []cdc.Record
	for i := range 40 {
		batch = append(batch, create(0, int64(i), int64(i+1), "ok", 10, 1))
	}
	batch[7] = create(0, 7, 8, "negative", -5, 1)
	batch[31] = create(0, 31, 32, "negative", -9, 1)
	batch[20] = event(0, 20, cdc.OpCreate, 21, "null", `{"id":21,"owner":"nul\u0000byte","balance":1,"version":1}`)
	done := async(func() error { return f.deliver(ctxTimeout(t, time.Minute), batch) })
	if err := within(t, done, 30*time.Second, "the batch (last error: "+svc.Stats().LastError+")"); err != nil {
		t.Fatal(err)
	}
	got := p.readAccounts()
	if len(got) != 37 {
		t.Fatalf("37 good rows expected, got %d", len(got))
	}
	for _, bad := range []int64{8, 21, 32} {
		if _, ok := got[bad]; ok {
			t.Fatalf("row %d was rejected and must not be in the read db", bad)
		}
	}
	// The NUL-byte row cannot even be decoded, so it is parked first; the two the
	// database rejected are found by splitting. Each is parked exactly once.
	var offsets []int64
	for _, d := range f.deadLettered() {
		offsets = append(offsets, d.Offset)
	}
	slices.Sort(offsets)
	if !slices.Equal(offsets, []int64{7, 20, 31}) {
		t.Fatalf("parked offsets = %v, want [7 20 31]", offsets)
	}
	if p.checkpoint("g-bisect", 0) != 40 {
		t.Fatal("the checkpoint must reach the end of the batch")
	}
}

// What is dead-lettered carries the reason without the row's values.
func TestPoison_DeadLetterReasonHidesData(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-hide")
	start(t, newService(t, p, f))
	p.exec(p.read, `ALTER TABLE accounts ADD CONSTRAINT balance_ok CHECK (balance >= 0)`)
	secret := "TOP-SECRET-OWNER"
	if err := f.deliver(ctxTimeout(t, time.Minute), []cdc.Record{create(0, 0, 1, secret, -1, 1)}); err != nil {
		t.Fatal(err)
	}
	if len(f.deadLettered()) != 1 {
		t.Fatal("expected one dead letter")
	}
	for _, l := range f.deadCauses() {
		if strings.Contains(l, secret) {
			t.Fatalf("the cause leaked a value: %s", l)
		}
		if !strings.Contains(l, "23514") {
			t.Fatalf("the cause lost its SQLSTATE: %s", l)
		}
	}
}

// ---- DLQ publishing failure ------------------------------------------------

// While the dead-letter topic is unreachable the batch is neither committed nor
// skipped: the partition stalls, and finishes the moment the topic is back,
// with the poison record parked exactly once.
func TestDLQ_UnreachableStallsThenRecovers(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-dlq")
	svc := newService(t, p, f)
	start(t, svc)
	f.failDeadLetter(errors.New("broker down"))
	ctx := ctxTimeout(t, time.Minute)
	done := async(func() error {
		return f.deliver(ctx, []cdc.Record{create(0, 0, 1, "ok", 1, 1), poison(0, 1), create(0, 2, 3, "ok", 1, 1)})
	})
	if !stays(done, 700*time.Millisecond) {
		t.Fatal("the batch must stall while the dead-letter topic is down")
	}
	if p.checkpoint("g-dlq", 0) != 0 || f.committedOffset(0) != 0 {
		t.Fatal("nothing may be committed past an unparked poison record")
	}
	if f.deadAttempts.Load() < 2 {
		t.Fatal("the service must keep trying to park it")
	}
	f.failDeadLetter(nil)
	if err := within(t, done, 20*time.Second, "the batch after recovery"); err != nil {
		t.Fatal(err)
	}
	if d := f.deadLettered(); len(d) != 1 || d[0].Offset != 1 {
		t.Fatalf("dead letters = %+v", d)
	}
	if p.checkpoint("g-dlq", 0) != 3 || len(p.readAccounts()) != 2 {
		t.Fatal("the batch must complete once the topic is back")
	}
}
