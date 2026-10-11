//go:build integration

package sql_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// Banking schema evolution and end-to-end guarantees on the twenty-release
// history in banking_releases_test.go.

// checkBankData verifies the seeded data after the full upgrade.
func checkBankData(t *testing.T, s ecsql.SQLServices) {
	t.Helper()
	if n := count(t, s, "SELECT count(*) FROM customers"); n != 200 {
		t.Errorf("customers = %d, want 200", n)
	}
	if n := count(t, s, "SELECT count(*) FROM customers WHERE email LIKE '%+dup%'"); n != 20 {
		t.Errorf("deduplicated emails = %d, want 20", n)
	}
	if n := count(t, s, "SELECT count(DISTINCT email) FROM customers"); n != 200 {
		t.Errorf("distinct emails = %d, want 200", n)
	}
	if n := count(t, s, "SELECT count(*) FROM accounts WHERE number IS NULL"); n != 0 {
		t.Errorf("%d accounts without a number", n)
	}
	if m := bankMoney(t, s); m != bankSeedCents {
		t.Errorf("money = %d cents, want %d", m, bankSeedCents)
	}
	if n := count(t, s, "SELECT count(*) FROM accounts WHERE status = 'frozen'"); n != 67 { // ids with id % 3 == 1
		t.Errorf("frozen accounts = %d, want 67", n)
	}
	if n := count(t, s, "SELECT count(*) FROM account_balances WHERE ledger_cents <> balance_cents"); n != 0 {
		t.Errorf("%d accounts whose ledger does not match the balance", n)
	}
}

func TestSQLBanking_TwentyReleasesOfBreakingChanges(t *testing.T) {
	t.Parallel()
	_, s := bankAt(t, 3)
	ctx := ctxT(t)
	for i := 4; i <= len(bankReleases); i++ {
		if err := s.UpSteps(ctx, 1); err != nil {
			t.Fatalf("release %d (%s): %v", i, bankReleases[i-1].name, err)
		}
		if n := count(t, s, "SELECT count(*) FROM customers"); n != 200 {
			t.Fatalf("release %d: customers = %d", i, n)
		}
		if n := count(t, s, "SELECT count(*) FROM ledger"); n != 200 {
			t.Fatalf("release %d: ledger rows = %d", i, n)
		}
		if m := bankMoney(t, s); m != bankSeedCents {
			t.Fatalf("release %d: money = %d, want %d", i, m, bankSeedCents)
		}
	}
	checkBankData(t, s)
}

func TestSQLBanking_ProductionBehindBySeventeenReleasesUpgradesWithItsData(t *testing.T) {
	t.Parallel()
	e, s := bankAt(t, 3)
	auto := ecsql.NewSQLService(e.dsn, 2, 4, e.dir, true, e.out, nil, nil)
	if err := auto.Start(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	defer auto.Stop(context.Background())
	if err := auto.Run(ctxT(t)); err != nil {
		t.Fatalf("upgrading 17 releases at once: %v", err)
	}
	if v := version(t, s, e); v != "200" {
		t.Fatalf("version = %s", v)
	}
	checkBankData(t, s)
}

func TestSQLBanking_ReplayFromScratchThenRollBackEveryMigration(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, len(bankReleases))
	final := replayAndRollBack(t, e)
	for _, want := range []string{"view account_balances", "view customer_accounts", "trg ledger ledger_no_change", "fn CREATE OR REPLACE FUNCTION public.transfer", "type account_status active,frozen,closed"} {
		if !strings.Contains(final, want) {
			t.Errorf("final schema lacks %q", want)
		}
	}
	// Replaying again gives the identical schema.
	if again := replayAndRollBack(t, e); again != final {
		t.Fatalf("second replay differs:\n%s", diffLines(final, again))
	}
}

func TestMigrationE2E_HappyBootstrapFromAnEmptyDatabase(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, len(bankReleases))
	s := ecsql.NewSQLService(e.dsn, 2, 4, e.dir, true, e.out, nil, nil)
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if v := version(t, s, e); v != "200" {
		t.Fatalf("version = %s", v)
	}
	if got := tables(t, s); strings.Join(got, ",") != "accounts,customers,ledger,transfers" {
		t.Fatalf("tables = %v", got)
	}
	// Drift-free: the same files on another database give the same schema.
	other := newEnv(t)
	copyMigrations(t, e, other)
	o := other.started(t)
	if err := o.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if a, b := snapshot(t, s), snapshot(t, o); a != b {
		t.Fatalf("bootstrap is not reproducible:\n%s", diffLines(a, b))
	}
}

func TestMigrationE2E_HappyTransfersAreAtomic(t *testing.T) {
	t.Parallel()
	_, s := bankAt(t, len(bankReleases))
	a, b := account(t, s, 1000), account(t, s, 0)
	ctx := ctxT(t)
	before := count(t, s, "SELECT count(*) FROM ledger")
	if _, err := s.Client().ExecContext(ctx, "SELECT transfer(?, ?, ?)", a, b, 5000); err == nil {
		t.Fatal("overdraft transfer succeeded")
	}
	if balance(t, s, a) != 1000 || balance(t, s, b) != 0 || count(t, s, "SELECT count(*) FROM ledger") != before ||
		count(t, s, "SELECT count(*) FROM transfers") != 0 {
		t.Fatal("failed transfer left a trace")
	}
	if _, err := s.Client().ExecContext(ctx, "SELECT transfer(?, ?, ?)", a, b, 400); err != nil {
		t.Fatal(err)
	}
	if balance(t, s, a) != 600 || balance(t, s, b) != 400 {
		t.Fatal("balances wrong after transfer")
	}
	if n := count(t, s, "SELECT count(*) FROM ledger WHERE transfer_id IS NOT NULL"); n != 2 {
		t.Fatalf("ledger entries = %d, want 2", n)
	}
	if n := count(t, s, "SELECT sum(amount_cents)::int FROM ledger WHERE transfer_id IS NOT NULL"); n != 0 {
		t.Fatalf("transfer ledger entries sum to %d, want 0", n)
	}
}

func TestMigrationE2E_SadInvalidStatesAreRejectedByTheSchema(t *testing.T) {
	t.Parallel()
	_, s := bankAt(t, len(bankReleases))
	a, b := account(t, s, 100), account(t, s, 100)
	for name, q := range map[string]string{
		"negative balance":   "UPDATE accounts SET balance_cents = -1 WHERE id = $1",
		"zero ledger amount": "INSERT INTO ledger (account_id, amount_cents) VALUES ($1, 0)",
		"unknown account":    "INSERT INTO ledger (account_id, amount_cents) VALUES ($1 + 100000, 5)",
		"transfer to itself": "INSERT INTO transfers (from_account, to_account, amount_cents) VALUES ($1, $1, 5)",
		"zero transfer":      "SELECT transfer($1, $2, 0)",
		"negative transfer":  "SELECT transfer($1, $2, -5)",
		"duplicate number":   "UPDATE accounts SET number = (SELECT number FROM accounts WHERE id = $2) WHERE id = $1",
		"duplicate email":    "UPDATE customers SET email = 'c1@bank.test' WHERE id = (SELECT customer_id FROM accounts WHERE id = $1)",
		"unknown status":     "UPDATE accounts SET status = 'gone' WHERE id = $1",
		"missing currency":   "UPDATE accounts SET currency = NULL WHERE id = $1",
		"transfer to frozen": "UPDATE accounts SET status = 'frozen' WHERE id = $2; SELECT transfer($1, $2, 5)",
	} {
		args := []any{a, b}
		tx, err := s.Client().BeginTx(ctxT(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		var execErr error
		for stmt := range strings.SplitSeq(q, "; ") {
			if _, execErr = tx.ExecContext(ctxT(t), stmt, argsFor(stmt, args)...); execErr != nil {
				break
			}
		}
		_ = tx.Rollback()
		if execErr == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if balance(t, s, a) != 100 || balance(t, s, b) != 100 {
		t.Fatal("balances changed")
	}
}

// argsFor passes only the parameters a statement uses.
func argsFor(stmt string, args []any) []any {
	switch {
	case strings.Contains(stmt, "$2") && strings.Contains(stmt, "$1"):
		return args[:2]
	case strings.Contains(stmt, "$2"):
		return []any{args[1]}
	default:
		return args[:1]
	}
}

func TestMigrationE2E_SadLedgerIsImmutableAcrossMigrations(t *testing.T) {
	t.Parallel()
	e, s := bankAt(t, len(bankReleases))
	ctx := ctxT(t)
	immutable := func(when string) {
		t.Helper()
		for _, q := range []string{"UPDATE ledger SET amount_cents = amount_cents + 1", "DELETE FROM ledger"} {
			if _, err := s.Client().ExecContext(ctx, q); err == nil || !strings.Contains(err.Error(), "append-only") {
				t.Fatalf("%s: %q = %v, want append-only error", when, q, err)
			}
		}
	}
	immutable("at release 20")
	// Redo the ledger trigger's own release (11) and everything after it.
	if err := s.RollbackTo(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.Redo(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if v := version(t, s, e); v != "200" {
		t.Fatalf("version = %s", v)
	}
	immutable("after redoing release 11")
	if n := count(t, s, "SELECT count(*) FROM ledger"); n != 200 {
		t.Fatalf("ledger rows = %d", n)
	}
}

func TestMigrationE2E_HappyConcurrentTransfersPreserveMoney(t *testing.T) {
	t.Parallel()
	_, s := bankAt(t, len(bankReleases))
	const accounts, start = 10, 10_000
	ids := make([]int64, accounts)
	for i := range ids {
		ids[i] = account(t, s, start)
	}
	total := func() int {
		return count(t, s, "SELECT sum(balance_cents)::int FROM accounts WHERE id = ANY(?)", pgArray(ids))
	}
	before := total()
	var wg sync.WaitGroup
	for w := range 16 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(w), 7))
			for range 40 {
				from, to := ids[r.IntN(accounts)], ids[r.IntN(accounts)]
				if from == to {
					continue
				}
				_, err := s.Client().ExecContext(ctxT(t), "SELECT transfer(?, ?, ?)", from, to, 1+r.IntN(3000))
				if err != nil && !strings.Contains(err.Error(), "accounts_balance_nonneg") {
					t.Errorf("transfer: %v", err)
				}
			}
		})
	}
	wg.Wait()
	if after := total(); after != before {
		t.Fatalf("money changed: %d → %d", before, after)
	}
	if n := count(t, s, "SELECT count(*) FROM accounts WHERE balance_cents < 0"); n != 0 {
		t.Fatalf("%d negative balances", n)
	}
	if n := count(t, s, "SELECT count(*) FROM account_balances WHERE id = ANY(?) AND ledger_cents <> balance_cents - ?", pgArray(ids), start); n != 0 {
		t.Fatalf("%d accounts whose ledger disagrees with the balance", n)
	}
}

func TestMigrationE2E_HappyOnlineMigrationUnderLoad(t *testing.T) {
	t.Parallel()
	e, s := bankAt(t, len(bankReleases))
	a, b := account(t, s, 1_000_000), account(t, s, 1_000_000)
	// An online release takes its locks in the order the app does (transfer()
	// writes transfers, then ledger) and gives up rather than queue forever;
	// locking ledger first would deadlock with running transfers.
	e.write(t, 210, "ledger_memo", "SET LOCAL lock_timeout = '5s';\nLOCK TABLE transfers, ledger IN ACCESS EXCLUSIVE MODE;\n"+
		"ALTER TABLE ledger ADD COLUMN memo text;\nCREATE INDEX transfers_created_idx ON transfers (created_at);",
		"DROP INDEX transfers_created_idx;\nALTER TABLE ledger DROP COLUMN memo;")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var done, failed int
	var mu sync.Mutex
	for w := range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				from, to := a, b
				if w%2 == 1 {
					from, to = b, a
				}
				_, err := s.Client().ExecContext(ctxT(t), "SELECT transfer(?, ?, 1)", from, to)
				mu.Lock()
				if err != nil {
					failed++
					t.Errorf("transfer during migration: %v", err)
				} else {
					done++
				}
				mu.Unlock()
			}
		})
	}
	time.Sleep(100 * time.Millisecond)
	var err error
	for range 3 { // DDL can lose a deadlock to the traffic; deploys retry
		if err = s.Migrate(ctxT(t)); err == nil || !strings.Contains(err.Error(), "deadlock") {
			break
		}
	}
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("migration under load: %v", err)
	}
	if v := version(t, s, e); v != "210" {
		t.Fatalf("version = %s", v)
	}
	if done == 0 {
		t.Fatal("no transfers ran")
	}
	if balance(t, s, a)+balance(t, s, b) != 2_000_000 {
		t.Fatal("money changed during the migration")
	}
}

func TestMigrationE2E_HappyDataSurvivesTheMigrationLifecycle(t *testing.T) {
	t.Parallel()
	e, s := bankAt(t, len(bankReleases))
	a, b := account(t, s, 500), account(t, s, 0)
	ctx := ctxT(t)
	if _, err := s.Client().ExecContext(ctx, "SELECT transfer(?, ?, 200)", a, b); err != nil {
		t.Fatal(err)
	}
	check := func(when string) {
		t.Helper()
		if balance(t, s, a) != 300 || balance(t, s, b) != 200 || count(t, s, "SELECT count(*) FROM customers") != 202 {
			t.Fatalf("%s: data lost", when)
		}
	}
	_ = s.Stop(ctx)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	check("restart")
	if err := s.Redo(ctx); err != nil {
		t.Fatal(err)
	}
	check("redo")
	if err := s.RollbackSteps(ctx, 2); err != nil { // views and currency
		t.Fatal(err)
	}
	if err := s.UpSteps(ctx, 2); err != nil {
		t.Fatal(err)
	}
	check("rollback and reapply")
	e.write(t, 210, "nickname", "ALTER TABLE customers ADD COLUMN nickname text;", "ALTER TABLE customers DROP COLUMN nickname;")
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	check("new release")
}

func TestMigrationE2E_SadFailedMigrationUndoesDataChangesToo(t *testing.T) {
	t.Parallel()
	e, s := bankAt(t, len(bankReleases))
	before, snap := bankMoney(t, s), snapshot(t, s)
	e.write(t, 210, "bad_backfill", "UPDATE accounts SET balance_cents = 0;\nALTER TABLE accounts ADD COLUMN x int;\nSELECT 1/0;", "SELECT 1;")
	if err := s.Migrate(ctxT(t)); err == nil {
		t.Fatal("broken release applied")
	}
	if bankMoney(t, s) != before {
		t.Fatal("failed migration kept its data changes")
	}
	if snapshot(t, s) != snap {
		t.Fatal("failed migration kept its schema changes")
	}
	if v := version(t, s, e); v != "200" {
		t.Fatalf("version = %s", v)
	}
}

func TestMigrationE2E_SadOutOfOrderMigrationIsRefusedThenResolved(t *testing.T) {
	t.Parallel()
	e, s := bankAt(t, len(bankReleases))
	ctx := ctxT(t)
	e.write(t, 105, "late_branch", "ALTER TABLE customers ADD COLUMN branch text;", "ALTER TABLE customers DROP COLUMN branch;")
	if err := s.Migrate(ctx); err == nil {
		t.Fatal("out-of-order migration accepted")
	}
	if err := os.Rename(filepath.Join(e.path, "105_late_branch.sql"), filepath.Join(e.path, "210_late_branch.sql")); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("after renumbering: %v", err)
	}
	if v := version(t, s, e); v != "210" {
		t.Fatalf("version = %s", v)
	}
}

func TestMigrationE2E_HappyFullLifecycleExercisesEveryServiceMethod(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, len(bankReleases))
	s := ecsql.NewSQLService(e.dsn, 2, 4, e.dir, true, e.out, nil, nil)
	ctx := ctxT(t)
	must := func(name string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	must("Create", s.Create(ctx, "audit note"))
	must("Start", s.Start(ctx))
	defer s.Stop(context.Background())
	must("Ping", s.Ping(ctx))
	must("Run", s.Run(ctx))
	a, b := account(t, s, 900), account(t, s, 0)
	if _, err := s.Client().ExecContext(ctx, "SELECT transfer(?, ?, 300)", a, b); err != nil {
		t.Fatal(err)
	}
	must("Status", s.Status(ctx))
	if !strings.Contains(e.out.String(), "audit_note.sql") {
		t.Fatal("Status does not list the created migration")
	}
	must("Version", s.Version(ctx))
	must("Rollback", s.Rollback(ctx))              // the created no-op
	must("RollbackSteps", s.RollbackSteps(ctx, 2)) // currency view, currency
	must("UpSteps", s.UpSteps(ctx, 2))
	must("Redo", s.Redo(ctx))
	must("RollbackTo", s.RollbackTo(ctx, 190))
	must("Migrate", s.Migrate(ctx))
	if balance(t, s, a) != 600 || balance(t, s, b) != 300 {
		t.Fatal("data lost across the lifecycle")
	}
	must("Fresh", s.Fresh(ctx))
	if n := count(t, s, "SELECT count(*) FROM accounts"); n != 0 {
		t.Fatalf("accounts after Fresh = %d", n)
	}
	must("Stop", s.Stop(ctx))
	if err := s.Ping(ctx); !errors.Is(err, ecsql.ErrNotStarted) {
		t.Fatalf("Ping after Stop = %v", err)
	}
}
