package regressions

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
)

// 13 Several instances of the app (behind the load balancer) on the same databases: they may boot
// at the same moment, they must refuse to run against a half-migrated pair, and their runners may
// receive interleaved and even duplicated changes.

func TestBankDBMultiInstance_SimultaneousBootsMigrateOnce(t *testing.T) {
	h := newDBHarness(t)
	writeBankMigration(t)
	const instances = 4
	svcs := make([]*database.DatabaseService, instances)
	for i := range svcs {
		svcs[i] = newBDService(t, h, bdOpts{maxOpen: 4, app: fmt.Sprintf("boot%d", i), route: func(d string) string { return d }, noRun: true})
		t.Cleanup(func() { _ = svcs[i].Stop(bg) })
	}
	var wg sync.WaitGroup
	errs := make([]error, instances)
	for i, svc := range svcs {
		wg.Go(func() { errs[i] = svc.Start(bg) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("instance %d failed to boot: %v", i, err)
		}
	}
	for name, db := range map[string]*sql.DB{"writer": h.writer, "reader": h.reader} {
		if n := count(t, db, `SELECT count(*) FROM goose_db_version WHERE version_id > 0`); n != 2 {
			t.Errorf("%s has %d migration records after %d simultaneous boots; want 2", name, n, instances)
		}
	}
	accounts, err := database.Get[bdAccount, bdAccountRes, bdNoRequest, string](svcs[3])
	must(t, err)
	must(t, second(accounts.Create(withDeadline(t, 5*time.Second), bdAccount{ID: "a", Owner: "Ann", Balance: 1})))
}

func TestBankDBMultiInstance_HalfMigratedPairRefusesToStart(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	ctx := withDeadline(t, 10*time.Second)
	// A newer release migrated the writer only (its reader migration failed or was skipped).
	must(t, os.WriteFile(filepath.Join("src", "database", "migrations", "00003_add_nickname.sql"),
		[]byte(gooseBody(`ALTER TABLE bank_accounts ADD COLUMN nickname text;`, `ALTER TABLE bank_accounts DROP COLUMN nickname;`)), 0o644))
	must(t, b.svc.Writer().Migrate(ctx))
	must(t, b.svc.Stop(ctx))

	stale := newBDService(t, b.h, bdOpts{maxOpen: 2, app: "stale", route: func(d string) string { return d }, noAutoMigrate: true})
	err := stale.Start(ctx)
	if !errors.Is(err, database.ErrSchemaMismatch) {
		t.Fatalf("Start on writer v3 / reader v2 = %v; want ErrSchemaMismatch", err)
	}
	if _, err := database.Get[bdAccount, bdAccountRes, bdNoRequest, string](stale); !errors.Is(err, database.ErrNotStarted) {
		t.Fatalf("Get after the refused start = %v; want ErrNotStarted", err)
	}
	// Running the reader migration repairs it.
	must(t, b.svc.Start(ctx)) // auto-migrates the reader to v3
	if err := stale.Start(ctx); err != nil {
		t.Fatalf("Start once both are at v3: %v", err)
	}
	t.Cleanup(func() { _ = stale.Stop(bg) })
}

func TestBankDBMultiInstance_InterleavedAndDuplicatedChangesConverge(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	// Two replicas, each with its own subscription, as during a consumer-group rebalance.
	var handlers []func(key, value []byte) error
	for i := range 2 {
		brk := &dbBroker{handlers: map[string]func(key, value []byte) error{}}
		svc := newBDService(t, b.h, bdOpts{maxOpen: 8, app: fmt.Sprintf("replica%d", i), route: func(d string) string { return d }, broker: brk})
		must(t, svc.Start(bg))
		t.Cleanup(func() { _ = svc.Stop(bg) })
		svc.Run(bg)
		handlers = append(handlers, brk.await(t, "bank_accounts"))
	}

	const accounts, versions = 5, 40
	errs := runParallel(t, accounts*versions, func(ctx context.Context, i int) error {
		id, v := fmt.Sprintf("a%d", i%accounts), int64(i/accounts+1)
		acc := bdAccount{ID: id, Owner: "o-" + id, Balance: v * 10, Version: v, UpdatedAt: time.Unix(1_700_000_000+v, 0)}
		event := fmt.Sprintf("%s:v%d", id, v)
		// Every change reaches one replica; a third of them reach both (redelivery).
		if err := publishTo(handlers[i%2], event, acc); err != nil {
			return err
		}
		if i%3 == 0 {
			return publishTo(handlers[(i+1)%2], event, acc)
		}
		return nil
	})
	if len(errs) > 0 {
		t.Fatalf("delivery failed: %v", errs[0])
	}
	b.awaitReader(`SELECT count(*) FROM bank_accounts WHERE version = 40`, accounts)
	// Older versions may still be queued after the newest landed; every distinct change is recorded once.
	b.awaitReader(`SELECT count(*) FROM processed_events`, accounts*versions)
	if n := count(t, b.h.reader, `SELECT count(*) FROM bank_accounts WHERE balance <> version * 10`); n != 0 {
		t.Fatalf("%d accounts mix fields from different versions", n)
	}
}

func publishTo(handler func(key, value []byte) error, eventID string, acc bdAccount) error {
	value, err := json.Marshal(cqrs.CQRSQueuePayload[bdAccount]{EventID: eventID, ChangeType: cqrs.ChangeTypeUpdated, Payload: acc})
	if err != nil {
		return err
	}
	return handler([]byte(eventID), value)
}

func TestBankDBMultiInstance_UnmigratedReaderRefusesToStart(t *testing.T) {
	h := newDBHarness(t)
	writeBankMigration(t)
	// Only the writer is migrated (e.g. a deploy whose reader step never ran).
	writerOnly := newBDService(t, h, bdOpts{maxOpen: 2, app: "w-only", route: func(d string) string { return d }, noRun: true})
	must(t, writerOnly.Start(bg)) // auto-migrates both: undo the reader's history to simulate the gap
	must(t, writerOnly.Stop(bg))
	_, err := h.reader.Exec(`DROP TABLE goose_db_version`)
	must(t, err)

	svc := newBDService(t, h, bdOpts{maxOpen: 2, app: "unmigrated", route: func(d string) string { return d }, noAutoMigrate: true})
	if err := svc.Start(bg); !errors.Is(err, database.ErrSchemaMismatch) {
		_ = svc.Stop(bg)
		t.Fatalf("Start with a migrated writer and an unmigrated reader = %v; want ErrSchemaMismatch", err)
	}
}
