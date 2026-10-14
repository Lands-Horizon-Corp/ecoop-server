//go:build integration

package cqrs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// ---- zero value, absence value, dangling pointer ---------------------------

func TestZero_UnusableServicesNeverPanic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	zero := &cqrsSvc{}
	if err := zero.Run(ctx); !errors.Is(err, cqrs.ErrNotConstructed) {
		t.Errorf("zero Run = %v", err)
	}
	if err := zero.Stop(ctx); err != nil {
		t.Errorf("zero Stop = %v", err)
	}
	if (zero.Stats() != cqrs.Stats{}) {
		t.Error("zero Stats")
	}
	if _, err := zero.StartTx(ctx); !errors.Is(err, cqrs.ErrWriteDBNotInitialized) {
		t.Errorf("zero StartTx = %v", err)
	}
	if err := zero.EndTx(ctx, bun.Tx{}, nil); !errors.Is(err, cqrs.ErrNilTx) {
		t.Errorf("EndTx on a transaction that was never started = %v", err)
	}
	boom := errors.New("boom")
	if err := zero.EndTx(ctx, bun.Tx{}, boom); !errors.Is(err, cqrs.ErrNilTx) || !errors.Is(err, boom) {
		t.Errorf("EndTx must keep both errors: %v", err)
	}

	var ptr *cqrsSvc
	if err := ptr.Run(ctx); !errors.Is(err, cqrs.ErrNotConstructed) {
		t.Errorf("nil Run = %v", err)
	}
	if err := ptr.Stop(ctx); err != nil {
		t.Errorf("nil Stop = %v", err)
	}
	if (ptr.Stats() != cqrs.Stats{}) {
		t.Error("nil Stats")
	}
	// Hooks on a nil or zero service, with nil data, do nothing instead of crashing.
	ptr.OnCreated(ctx, &Account{})
	ptr.OnUpdated(ctx, nil)
	ptr.OnDeleted(ctx, &Account{})
	zero.OnCreated(ctx, &Account{ID: 1})
	zero.OnUpdated(ctx, nil)
}

func TestZero_NilContextAndMissingServices(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-nil")
	svc := newService(t, p, f)
	var nilCtx context.Context
	if err := svc.Run(nilCtx); !errors.Is(err, cqrs.ErrNilContext) {
		t.Errorf("Run(nil ctx) = %v", err)
	}
	if err := svc.Stop(nilCtx); !errors.Is(err, cqrs.ErrNilContext) {
		t.Errorf("Stop(nil ctx) = %v", err)
	}
	if _, err := svc.StartTx(nilCtx); !errors.Is(err, cqrs.ErrNilContext) {
		t.Errorf("StartTx(nil ctx) = %v", err)
	}
	if f.subscribed() {
		t.Error("a refused Run must not subscribe")
	}

	mustPanic := func(name string, mod func(*cqrsSvc)) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: NewCQRS must refuse it at construction", name)
			}
		}()
		newService(t, p, f, mod)
	}
	mustPanic("nil write service", func(c *cqrsSvc) { c.WriteSQLService = nil })
	mustPanic("nil cdc service", func(c *cqrsSvc) { c.CDCService = nil })
	mustPanic("nil pagination", func(c *cqrsSvc) { c.PaginationService = nil })
	mustPanic("unknown id column", func(c *cqrsSvc) { c.ColumnDefaultID = "nope" })
	mustPanic("unknown version column", func(c *cqrsSvc) { c.ColumnVersion = "nope" })
}

// No read database configured: replication has nowhere to go, and says so.
func TestZero_NoReadDatabase(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-noread")
	svc := newService(t, p, f, func(c *cqrsSvc) { c.ReadSQLService = nil })
	if err := svc.Run(context.Background()); !errors.Is(err, cqrs.ErrReadDBNotInitialized) {
		t.Fatalf("Run = %v", err)
	}
}

// Absent and null everywhere in a record: none of it panics, and each is
// classified the same way (a tombstone is skipped, a null row is poison).
func TestZero_AbsentValuesInRecords(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-absent")
	svc := newService(t, p, f, func(c *cqrsSvc) {
		c.ToResource = func(*Account) *AccountResponse { return nil } // a hook mapper returning nil
		c.Created = func(*Account) []string { return []string{"c"} }
	})
	start(t, svc)
	nilEvent := cdc.Record{Topic: testTopic, Partition: 0, Offset: 3, Key: nil, Event: &cdc.ChangeEvent{Op: cdc.OpCreate}}
	batch := []cdc.Record{
		tombstone(0, 0, 1),
		event(0, 1, cdc.OpCreate, 2, "null", "null"),
		event(0, 2, cdc.OpCreate, 3, "null", ""),
		nilEvent,
		create(0, 4, 5, "fine", 1, 1),
	}
	if err := f.deliver(ctxTimeout(t, time.Minute), batch); err != nil {
		t.Fatal(err)
	}
	if got := p.readAccounts(); len(got) != 1 || got[5].Owner != "fine" {
		t.Fatalf("read db = %+v", got)
	}
	s := svc.Stats()
	if s.Tombstones != 1 || s.DeadLettered != 3 {
		t.Fatalf("tombstones=%d dead-lettered=%d, want 1 and 3", s.Tombstones, s.DeadLettered)
	}
	if err := f.deliver(ctxTimeout(t, time.Minute), []cdc.Record{{Topic: testTopic, Offset: 5}}); err != nil {
		t.Fatalf("a bare record is a tombstone: %v", err)
	}
}

// ---- smoke -----------------------------------------------------------------

func TestSmoke_ConstructRunStop(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	f := newFakeCDC("g-smoke")
	svc := newService(t, p, f, func(c *cqrsSvc) { c.Topic = "" })
	if err := svc.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.topic != testTopic {
		t.Fatalf("the topic must come from the model: %q, want %q", f.topic, testTopic)
	}
	if (svc.Stats() != cqrs.Stats{}) {
		t.Fatalf("a fresh service has empty stats: %+v", svc.Stats())
	}
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSmoke_CheckpointDDLIsIdempotent(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	for range 3 {
		p.exec(p.read, cqrs.CheckpointDDL)
	}
}

// StartTx/EndTx: commit on success, roll back on error, and the transaction is
// always closed.
func TestSmoke_StartAndEndTx(t *testing.T) {
	t.Parallel()
	p := newPair(t, 4)
	svc := newService(t, p, newFakeCDC("g-tx"))
	ctx := context.Background()

	tx, err := svc.StartTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO accounts (id, owner) VALUES (1, 'committed')`); err != nil {
		t.Fatal(err)
	}
	if err := svc.EndTx(ctx, tx, nil); err != nil {
		t.Fatal(err)
	}
	tx, err = svc.StartTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = tx.ExecContext(ctx, `INSERT INTO accounts (id, owner) VALUES (2, 'rolled back')`)
	boom := errors.New("boom")
	if err := svc.EndTx(ctx, tx, boom); !errors.Is(err, boom) {
		t.Fatalf("EndTx must return the error it was given: %v", err)
	}
	if n := p.count(p.write, "accounts"); n != 1 {
		t.Fatalf("write db rows = %d, want only the committed one", n)
	}
	if err := svc.EndTx(ctx, tx, nil); err == nil {
		t.Fatal("ending a transaction twice must report it, not panic")
	}
}

// ---- integration chaos -----------------------------------------------------

// The consumer dies in the middle of a live stream, with no goodbye; a new
// instance of the same group takes over from the checkpoint, and no change is
// lost or applied twice, with real Postgres, Debezium and Kafka.
func TestIntegrationChaos_ConsumerDiesDuringLiveChanges(t *testing.T) {
	t.Parallel()
	s := skipUnlessDebezium(t)
	p := newPair(t, 16)
	prefix := s.connector(t, p)
	group := fmt.Sprintf("g-ichaos-%d", runID)

	cdcA := s.realCDC(t, group, prefix)
	svcA := realService(t, p, cdcA)
	start(t, svcA)

	const n = 80
	for i := 1; i <= n; i++ {
		p.exec(p.write, fmt.Sprintf(`INSERT INTO accounts (id, owner, balance, version) VALUES (%d, 'row-%d', %d, 1)`, i, i, i))
		if i == n/4 {
			// The consumer is gone: the service never learns, its batch may be mid-flight.
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			_ = cdcA.Stop(ctx)
			cancel()
			cdcB := s.realCDC(t, group, prefix)
			start(t, realService(t, p, cdcB))
		}
	}
	eventually(t, "every insert on the read side", func() bool { return p.count(p.read, "accounts") == n })
	got := p.readAccounts()
	for i := int64(1); i <= n; i++ {
		if got[i].Owner != fmt.Sprintf("row-%d", i) || got[i].Balance != i {
			t.Fatalf("row %d = %+v", i, got[i])
		}
	}
}
