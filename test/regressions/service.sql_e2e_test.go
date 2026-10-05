package regressions

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
)

// Migration end-to-end tests: the whole path from an empty database to a loaded, evolving bank,
// checking the ACID properties a ledger needs at each step of the migration lifecycle.

// Happy: a brand-new environment is built from nothing into the full, hardened, drift-free schema.
func TestMigrationE2E_HappyBootstrapFromAnEmptyDatabase(t *testing.T) {
	e := newSQLEnv(t)
	if e.schema() != "" {
		t.Fatal("the test database is not empty")
	}
	bootstrapBank(t, e)

	if got := e.countTables(bankTables); got != len(bankTables) {
		t.Fatalf("%d of %d tables created", got, len(bankTables))
	}
	if n := e.scanInt(`SELECT count(*) FROM pg_constraint WHERE contype = 'f'`); n < 15 {
		t.Fatalf("%d foreign keys; the relations were not created", n)
	}
	if n := e.scanInt(`SELECT count(*) FROM pg_constraint WHERE contype = 'u'`); n < 8 {
		t.Fatalf("%d unique constraints", n)
	}
	if n := e.scanInt(`SELECT count(*) FROM pg_constraint WHERE contype = 'c'`); n < 8 {
		t.Fatalf("%d CHECK constraints; hardening is missing", n)
	}
	for _, idx := range []string{"ledger_entries_account_idx", "cards_one_active_per_account"} {
		if e.scanInt(`SELECT count(*) FROM pg_indexes WHERE indexname = $1`, idx) != 1 {
			t.Fatalf("index %s is missing", idx)
		}
	}
	requireConverged(t, e, bankModels()...)
	seedBank(t, e, 3, 10_000)
	requireBankInvariants(t, e, 3, 10_000)
}

// Happy: atomicity. A transfer that cannot complete leaves no trace; one that can leaves exactly two entries.
func TestMigrationE2E_HappyTransfersAreAtomic(t *testing.T) {
	e := newSQLEnv(t)
	bootstrapBank(t, e)
	ids := seedBank(t, e, 2, 1_000)

	if err := bankTransfer(bg, e.db, "too-much", ids[0], ids[1], 5_000); !errors.Is(err, errInsufficientFunds) {
		t.Fatalf("overdrawing transfer = %v; want errInsufficientFunds", err)
	}
	for table, want := range map[string]int64{"transactions": 0, "ledger_entries": 0, "transfers": 0} {
		if got := e.scanInt(`SELECT count(*) FROM ` + table); got != want {
			t.Fatalf("a rolled-back transfer left %d rows in %s", got, table)
		}
	}
	if e.scanInt(`SELECT balance FROM accounts WHERE id = $1`, ids[0]) != 1_000 {
		t.Fatal("a rolled-back transfer changed a balance")
	}

	if err := bankTransfer(bg, e.db, "ok", ids[0], ids[1], 400); err != nil {
		t.Fatal(err)
	}
	if got := e.scanInt(`SELECT count(*) FROM ledger_entries`); got != 2 {
		t.Fatalf("a committed transfer wrote %d ledger entries; want 2", got)
	}
	// The same idempotency key must not post twice.
	if err := bankTransfer(bg, e.db, "ok", ids[0], ids[1], 400); pgCode(err) != "23505" {
		t.Fatalf("replayed idempotency key = %v; want a unique violation", err)
	}
	requireBankInvariants(t, e, 2, 1_000)
}

// Sad: consistency. The database itself refuses every invalid state; nothing relies on application code.
func TestMigrationE2E_SadInvalidStatesAreRejectedByTheSchema(t *testing.T) {
	e := newSQLEnv(t)
	bootstrapBank(t, e)
	ids := seedBank(t, e, 2, 1_000)
	a, b := ids[0], ids[1]
	e.exec(`INSERT INTO cards (account_id, pan_hash, status, expires_at) VALUES ($1, 'hash-1', 'active', now() + interval '1 year')`, a)

	newTx := func(tx *sql.Tx, key string) int64 {
		var id int64
		if err := tx.QueryRow(`INSERT INTO transactions (idempotency_key, kind, status) VALUES ($1, 'transfer', 'posted') RETURNING id`, key).Scan(&id); err != nil {
			t.Fatalf("setup: %v", err)
		}
		return id
	}
	cases := []struct {
		name string
		code string
		run  func(tx *sql.Tx) error
	}{
		{"negative balance", "23514", func(tx *sql.Tx) error {
			_, err := tx.Exec(`UPDATE accounts SET balance = -1 WHERE id = $1`, a)
			return err
		}},
		{"bad currency length", "23514", func(tx *sql.Tx) error {
			_, err := tx.Exec(`UPDATE accounts SET currency = 'US' WHERE id = $1`, a)
			return err
		}},
		{"unknown status", "23514", func(tx *sql.Tx) error {
			_, err := tx.Exec(`UPDATE accounts SET status = 'weird' WHERE id = $1`, a)
			return err
		}},
		{"zero amount entry", "23514", func(tx *sql.Tx) error {
			_, err := tx.Exec(`INSERT INTO ledger_entries (transaction_id, account_id, direction, amount) VALUES ($1, $2, 'D', 0)`, newTx(tx, "k1"), a)
			return err
		}},
		{"bad direction", "23514", func(tx *sql.Tx) error {
			_, err := tx.Exec(`INSERT INTO ledger_entries (transaction_id, account_id, direction, amount) VALUES ($1, $2, 'X', 5)`, newTx(tx, "k2"), a)
			return err
		}},
		{"self transfer", "23514", func(tx *sql.Tx) error {
			_, err := tx.Exec(`INSERT INTO transfers (transaction_id, from_account_id, to_account_id, amount) VALUES ($1, $2, $2, 5)`, newTx(tx, "k3"), a)
			return err
		}},
		{"ledger for an unknown account", "23503", func(tx *sql.Tx) error {
			_, err := tx.Exec(`INSERT INTO ledger_entries (transaction_id, account_id, direction, amount) VALUES ($1, 999999, 'D', 5)`, newTx(tx, "k4"))
			return err
		}},
		{"duplicate idempotency key", "23505", func(tx *sql.Tx) error {
			newTx(tx, "dup")
			_, err := tx.Exec(`INSERT INTO transactions (idempotency_key, kind, status) VALUES ('dup', 'transfer', 'posted')`)
			return err
		}},
		{"duplicate tax id", "23505", func(tx *sql.Tx) error {
			_, err := tx.Exec(`INSERT INTO customers (branch_id, legal_name, tax_id, kyc_status) VALUES (1, 'Copy', 'TAX-00000', 'verified')`)
			return err
		}},
		{"second active card", "23505", func(tx *sql.Tx) error {
			_, err := tx.Exec(`INSERT INTO cards (account_id, pan_hash, status, expires_at) VALUES ($1, 'hash-2', 'active', now() + interval '1 year')`, a)
			return err
		}},
		{"unbalanced ledger at commit", "P0001", func(tx *sql.Tx) error {
			_, err := tx.Exec(`INSERT INTO ledger_entries (transaction_id, account_id, direction, amount) VALUES ($1, $2, 'D', 500)`, newTx(tx, "k5"), a)
			return err // the deferred balance check fires at COMMIT
		}},
	}
	for _, c := range cases {
		tx, err := e.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		err = c.run(tx)
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		if got := pgCode(err); got != c.code {
			t.Errorf("%s: SQLSTATE %q (%v); want %s", c.name, got, err, c.code)
		}
	}
	_ = b
	requireBankInvariants(t, e, 2, 1_000)
}

// Sad: the ledger is append-only, and stays that way after its own migration is redone.
func TestMigrationE2E_SadLedgerIsImmutableAcrossMigrations(t *testing.T) {
	e := newSQLEnv(t)
	svc := bootstrapBank(t, e)
	ids := seedBank(t, e, 2, 10_000)
	if err := bankTransfer(bg, e.db, "immutable", ids[0], ids[1], 100); err != nil {
		t.Fatal(err)
	}
	entries := e.scanInt(`SELECT count(*) FROM ledger_entries`)

	check := func(stage string) {
		for _, stmt := range []string{`UPDATE ledger_entries SET amount = amount + 1`, `DELETE FROM ledger_entries`} {
			_, err := e.db.Exec(stmt)
			if err == nil || !strings.Contains(err.Error(), "immutable") {
				t.Fatalf("%s: %q = %v; want the immutability error", stage, stmt, err)
			}
		}
		if e.scanInt(`SELECT count(*) FROM ledger_entries`) != entries {
			t.Fatalf("%s: ledger rows changed", stage)
		}
	}
	check("after bootstrap")
	if err := svc.Redo(bg); err != nil {
		t.Fatalf("Redo of the hardening migration: %v", err)
	}
	check("after Redo")
	requireBankInvariants(t, e, 2, 10_000)
}

// Happy: isolation. Many concurrent transfers never create or destroy money and never go negative.
func TestMigrationE2E_HappyConcurrentTransfersPreserveMoney(t *testing.T) {
	e := newSQLEnv(t)
	bootstrapBank(t, e)
	ids := seedBank(t, e, 6, 100_000)

	committed, failures := transferLoad(e, ids, 8, 30, nil)
	for _, err := range failures {
		t.Errorf("unexpected transfer failure: %v", err)
	}
	if committed == 0 {
		t.Fatal("no transfer committed")
	}
	if got := e.scanInt(`SELECT count(*) FROM transfers`); got != committed {
		t.Fatalf("%d transfers recorded for %d commits", got, committed)
	}
	requireBankInvariants(t, e, 6, 100_000)
}

// Happy: a release applied while transfers are running does not break a single one.
// DDL queued behind row locks can deadlock with a busy workload, so the migrator retries, as in production.
func TestMigrationE2E_HappyOnlineMigrationUnderLoad(t *testing.T) {
	e := newSQLEnv(t)
	live := bootstrapBank(t, e)
	ids := seedBank(t, e, 6, 100_000)

	// Author the release on a scratch environment, without load, so the files exist before the rollout.
	scratch := e.sibling()
	scratch.running() // applies the baseline, so the release diff has nothing pending
	settleModels(t, scratch, "release v2", bankModelsV2())

	stop := make(chan struct{})
	var (
		wg        sync.WaitGroup
		committed int64
		failures  []error
	)
	wg.Go(func() {
		committed, failures = transferLoad(e, ids, 6, 0, stop)
	})
	time.Sleep(200 * time.Millisecond)

	var migrateErr error
	for range 10 {
		if migrateErr = live.Migrate(bg); migrateErr == nil || !strings.Contains(migrateErr.Error(), "deadlock") {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	if migrateErr != nil {
		t.Fatalf("online migration: %v", migrateErr)
	}
	for _, err := range failures {
		t.Errorf("a transfer failed during the migration: %v", err)
	}
	if committed == 0 {
		t.Fatal("no transfers committed while migrating")
	}
	if !e.hasColumn("accounts", "risk_score") || !e.hasTable("risk_profiles") {
		t.Fatal("the release was not applied")
	}
	requireBankInvariants(t, e, 6, 100_000)
}

// Happy: durability. Committed data survives restarts, redo, rollback-and-reapply, and a schema release.
func TestMigrationE2E_HappyDataSurvivesTheMigrationLifecycle(t *testing.T) {
	e := newSQLEnv(t)
	svc := bootstrapBank(t, e)
	ids := seedBank(t, e, 4, 70_000)
	for i := range 5 {
		if err := bankTransfer(bg, e.db, fmt.Sprintf("durable-%d", i), ids[i%4], ids[(i+1)%4], 2_000); err != nil {
			t.Fatal(err)
		}
	}
	want := accountsChecksum(e)
	same := func(stage string) {
		t.Helper()
		if got := accountsChecksum(e); got != want {
			t.Fatalf("%s changed committed data", stage)
		}
		requireBankInvariants(t, e, 4, 70_000)
	}

	if err := svc.Stop(bg); err != nil {
		t.Fatal(err)
	}
	restarted := e.running()
	var n int64
	if err := restarted.Client().QueryRowContext(bg, `SELECT count(*) FROM ledger_entries`).Scan(&n); err != nil || n != 10 {
		t.Fatalf("after a restart the service sees %d ledger entries, %v; want 10", n, err)
	}
	same("a restart")

	if err := restarted.Redo(bg); err != nil {
		t.Fatal(err)
	}
	same("Redo")
	if err := restarted.RollbackSteps(bg, 1); err != nil {
		t.Fatal(err)
	}
	if err := restarted.UpSteps(bg, 1); err != nil {
		t.Fatal(err)
	}
	same("a rollback and re-apply")
	settleModels(t, e, "release v2", bankModelsV2())
	same("a schema release")
}

// Sad: a migration that mutates data and then fails undoes all of it, including the data changes.
func TestMigrationE2E_SadFailedMigrationUndoesDataChangesToo(t *testing.T) {
	e := newSQLEnv(t)
	svc := bootstrapBank(t, e)
	seedBank(t, e, 3, 25_000)
	good, sum := e.appliedVersion(), accountsChecksum(e)

	bad := e.writeNext("half_done",
		"UPDATE accounts SET balance = balance + 1;\nALTER TABLE accounts ADD COLUMN half_done text;\nINSERT INTO nonexistent_table VALUES (1);",
		"ALTER TABLE accounts DROP COLUMN half_done;")
	if err := svc.Migrate(bg); err == nil {
		t.Fatal("Migrate succeeded with a failing statement")
	}
	if e.appliedVersion() != good || e.hasColumn("accounts", "half_done") || accountsChecksum(e) != sum {
		t.Fatal("a failed migration left schema, data or version changed")
	}
	requireBankInvariants(t, e, 3, 25_000)

	if err := os.Remove(bad); err != nil {
		t.Fatal(err)
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("Migrate after removing the broken file: %v", err)
	}
}

// Sad: a migration whose version is older than what is applied is refused, and resolved by renumbering.
func TestMigrationE2E_SadOutOfOrderMigrationIsRefusedThenResolved(t *testing.T) {
	e := newSQLEnv(t)
	svc := bootstrapBank(t, e)
	seedBank(t, e, 2, 5_000)
	good, sum := e.appliedVersion(), accountsChecksum(e)

	late := filepath.Join(e.dir, "00001_late_hotfix.sql")
	if err := os.WriteFile(late, []byte("-- +goose Up\nCREATE TABLE hotfix_notes (id int);\n\n-- +goose Down\nDROP TABLE hotfix_notes;\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := svc.Migrate(bg); err == nil {
		t.Fatal("Migrate applied a migration older than the current version")
	}
	if e.hasTable("hotfix_notes") || e.appliedVersion() != good || accountsChecksum(e) != sum {
		t.Fatal("the refused migration changed the database")
	}

	if err := os.Remove(late); err != nil {
		t.Fatal(err)
	}
	e.writeNext("late_hotfix", `CREATE TABLE hotfix_notes (id int);`, `DROP TABLE hotfix_notes;`)
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("Migrate of the renumbered migration: %v", err)
	}
	if !e.hasTable("hotfix_notes") || e.appliedVersion() <= good {
		t.Fatal("the renumbered migration was not applied")
	}
}

// Happy: every SQLServices method, in one realistic lifecycle on the banking schema.
func TestMigrationE2E_HappyFullLifecycleExercisesEveryServiceMethod(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.newService()

	// Run, Ping, Client
	if err := svc.Run(bg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := svc.Ping(bg); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if svc.Client() == nil {
		t.Fatal("Client() is nil after Run")
	}

	// Create, Status, Migrate
	if err := svc.Create(bg, "scaffold"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	e.resetStatusOutput()
	if err := svc.Status(bg); err != nil || !strings.Contains(e.statusOutput(), "Pending") {
		t.Fatalf("Status should list the scaffold as pending: %v\n%s", err, e.statusOutput())
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	scaffoldVersion := e.appliedVersion()
	if scaffoldVersion == 0 {
		t.Fatal("the scaffold was not applied")
	}

	// Diff, Migrate: the released schema, then the hand-written hardening
	settleModels(t, e, "bank baseline", bankModels())
	baseChecks := e.scanInt(`SELECT count(*) FROM pg_constraint WHERE contype = 'c'`)
	e.writeNext("bank_hardening", bankHardeningUp, bankHardeningDown)
	if err := svc.Migrate(bg); err != nil {
		t.Fatal(err)
	}
	latest := e.appliedVersion()
	if got := e.versionOutput(svc); got != latest {
		t.Fatalf("Version = %d; want %d", got, latest)
	}
	built := e.schema()

	// RollbackSteps, UpSteps
	if err := svc.RollbackSteps(bg, 1); err != nil {
		t.Fatal(err)
	}
	if triggers, checks := e.triggerCount(), e.scanInt(`SELECT count(*) FROM pg_constraint WHERE contype = 'c'`); triggers != 0 || checks != baseChecks {
		t.Fatalf("RollbackSteps(1) did not remove the hardening (triggers=%d, checks=%d, want %d)", triggers, checks, baseChecks)
	}
	if err := svc.UpSteps(bg, 1); err != nil {
		t.Fatal(err)
	}
	if e.triggerCount() != 2 {
		t.Fatal("UpSteps(1) did not restore the hardening")
	}

	// Redo
	if err := svc.Redo(bg); err != nil || e.schema() != built {
		t.Fatalf("Redo: %v (schema changed: %v)", err, e.schema() != built)
	}

	// Real traffic, then a release
	ids := seedBank(t, e, 3, 40_000)
	if err := bankTransfer(bg, e.db, "lifecycle", ids[0], ids[1], 7_000); err != nil {
		t.Fatal(err)
	}
	want := accountsChecksum(e)
	settleModels(t, e, "release v2", bankModelsV2())
	if accountsChecksum(e) != want {
		t.Fatal("the release changed data")
	}

	// RollbackTo: back to just the scaffold
	if err := svc.RollbackTo(bg, scaffoldVersion); err != nil {
		t.Fatalf("RollbackTo: %v", err)
	}
	if e.countTables(bankTables) != 0 || e.appliedVersion() != scaffoldVersion {
		t.Fatalf("RollbackTo(%d) left %d bank tables at version %d", scaffoldVersion, e.countTables(bankTables), e.appliedVersion())
	}

	// Fresh: rebuild everything from the files, discarding data
	if err := svc.Fresh(bg); err != nil {
		t.Fatalf("Fresh: %v", err)
	}
	if e.countTables(bankTables) != len(bankTables) || e.scanInt(`SELECT count(*) FROM accounts`) != 0 {
		t.Fatal("Fresh did not rebuild an empty schema")
	}
	requireConverged(t, e, bankModelsV2()...)

	// Stop
	if err := svc.Stop(bg); err != nil {
		t.Fatal(err)
	}
	if err := svc.Ping(bg); !errors.Is(err, sqlsvc.ErrNotInitialized) {
		t.Fatalf("Ping after Stop = %v; want ErrNotInitialized", err)
	}
	_ = context.Background
}
