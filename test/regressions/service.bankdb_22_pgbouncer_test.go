package regressions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
)

// 22 PgBouncer in transaction pooling mode, as in production (`pgbouncer` compose service). Every
// statement may run on a different server connection, so nothing may depend on session state.

func TestBankDBPgBouncer_TransfersAndConcurrentReplaysWork(t *testing.T) {
	b := newBDBank(t, bdOpts{pgbouncer: true, noRun: true, maxOpen: 16})
	ids := []string{"a", "b", "c"}
	for _, id := range ids {
		b.open(id, 10_000)
	}
	var originals atomic.Int64
	errs := runParallel(t, 150, func(ctx context.Context, i int) error {
		// Every third request is a duplicate of another one: same key, same payload.
		key := fmt.Sprintf("k%03d", i-i%3)
		req := transferReq{Key: key, From: ids[(i-i%3)%3], To: ids[(i-i%3+1)%3], Amount: 5}
		_, replayed, err := b.Transfer(ctx, req)
		if err == nil && !replayed {
			originals.Add(1)
		}
		return err
	})
	if len(errs) > 0 {
		t.Fatalf("%d calls failed through PgBouncer, first: %v", len(errs), errs[0])
	}
	if originals.Load() != 50 || count(t, b.h.writer, `SELECT count(*) FROM bank_transfers`) != 50 {
		t.Fatalf("%d executed transfers; want 50 distinct keys executed once", originals.Load())
	}
	if total := b.writerTotal(); total != 30_000 {
		t.Fatalf("total = %d; want 30000", total)
	}
}

func TestBankDBPgBouncer_ServerTimeoutsApplyThroughThePooler(t *testing.T) {
	b := newBDBank(t, bdOpts{pgbouncer: true, noRun: true, svcOpts: []database.Option{
		database.WithServerTimeouts(database.ServerTimeouts{Statement: 300 * time.Millisecond}),
	}})
	_, err := b.h.reader.Exec(`INSERT INTO bank_accounts (id, owner, balance) VALUES ('r', 'r', 1)`)
	must(t, err)
	// PgBouncer may still hold server sessions opened before the setting; recycle them.
	_, _ = b.h.writer.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()`)
	_, _ = b.h.reader.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()`)

	started := time.Now()
	_, err = b.accounts.Find(bg, sleepFilter(10)) // no client deadline
	requireKind(t, err, database.ErrTimeout)
	if d := time.Since(started); d > 3*time.Second {
		t.Fatalf("statement ran %v through PgBouncer", d)
	}
}

func TestBankDBPgBouncer_MigrationsThroughThePoolerAreRefused(t *testing.T) {
	h := newDBHarness(t)
	writeBankMigration(t)
	svc := newBDService(t, h, bdOpts{maxOpen: 2, app: "pooled", route: func(d string) string { return rehost(d, pgbouncerAddr()) },
		svcOpts: []database.Option{database.WithPgBouncer()}})
	err := svc.Start(bg)
	if !errors.Is(err, database.ErrMigrateThroughPooler) {
		t.Fatalf("Start with auto-migrate through PgBouncer = %v; want ErrMigrateThroughPooler", err)
	}
}

func TestBankDBPgBouncer_NoPreparedStatementErrorsUnderMixedLoad(t *testing.T) {
	b := newBDBank(t, bdOpts{pgbouncer: true, noRun: true, maxOpen: 16})
	b.open("a", 1_000_000)
	b.open("b", 0)
	errs := runParallel(t, 300, func(ctx context.Context, i int) error {
		var err error
		switch i % 4 {
		case 0:
			_, _, err = b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%03d", i), From: "a", To: "b", Amount: 1})
		case 1:
			_, err = b.accounts.Count(ctx, eqFilter("owner", "owner-a"))
		case 2:
			_, err = b.accounts.IncrementByID(ctx, "b", "version", 0)
		case 3:
			_, err = b.accounts.Find(ctx, eqFilter("id", "b"))
		}
		return err
	})
	for _, err := range errs {
		if strings.Contains(err.Error(), "prepared statement") {
			t.Fatalf("prepared statement error through PgBouncer: %v", err)
		}
	}
	if len(errs) > 0 {
		t.Fatalf("%d operations failed, first: %v", len(errs), errs[0])
	}
}
