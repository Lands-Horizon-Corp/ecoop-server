//go:build integration

package sql_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// The ledger foundation: twenty releases of a cooperative bank's schema,
// written as Bun models and migrated by Migrate, with live data throughout.
// Every release must apply, converge (a second Migrate writes nothing), keep
// every row and every peso, and keep the indexes and checks the models
// declare in their AfterCreateTable hooks. The history covers new tables and relations,
// foreign keys on populated tables, a composite-key join table, a column and
// a table rename, a self-reference, two foreign keys to one table, a
// one-to-one, a composite unique key, a type change, a NOT NULL + UNIQUE the
// existing data violates, and reviewed column drops. Bun defines the
// schema, Atlas plans each migration, goose applies it.

type ledgerRelease struct {
	name   string
	models []any
	// hand is a hand-written migration (Up, Down) for what the models do
	// not describe: data backfills. It is applied before planning.
	hand [2]string
	// fix is the data fix a release needs when the existing data violates
	// the new models: Migrate must first refuse, then succeed after it.
	fix [2]string
	// review marks a release that drops a column: Migrate writes a .review
	// file and applies nothing until it is approved.
	review bool
	seed   string
	check  func(t *testing.T, s ecsql.ISQLService)
}

var (
	r1  = []any{(*LBranch)(nil), (*LMember)(nil)}
	r2  = append(slices.Clone(r1), (*LAccount)(nil))
	r3  = []any{(*LBranch)(nil), (*LMemberBranch)(nil), (*LAccount)(nil)}
	r4  = append(slices.Clone(r3), (*LEntry)(nil))
	r5  = []any{(*LBranch)(nil), (*LMemberBranchIdx)(nil), (*LAccount)(nil), (*LEntryIdx)(nil)}
	r6  = []any{(*LBranch)(nil), (*LMemberBranchIdx)(nil), (*LAccountCurrency)(nil), (*LEntryIdx)(nil)}
	r7  = append(slices.Clone(r6), (*LHolder)(nil))
	r9  = []any{(*LBranch)(nil), (*LMemberFullName)(nil), (*LAccountCurrency)(nil), (*LEntryIdx)(nil), (*LHolder)(nil)}
	r10 = append(slices.Clone(r9), (*LLoan)(nil), (*LPayment)(nil))
	r11 = []any{(*LBranch)(nil), (*LMemberFullName)(nil), (*LAccountCurrency)(nil), (*LEntryTransfer)(nil), (*LHolder)(nil), (*LLoan)(nil), (*LPayment)(nil), (*LTransfer)(nil)}
	r12 = []any{(*LBranch)(nil), (*LMemberFullName)(nil), (*LAccountChecked)(nil), (*LEntryChecked)(nil), (*LHolder)(nil), (*LLoan)(nil), (*LPayment)(nil), (*LTransferChecked)(nil)}
	r13 = []any{(*LBranch)(nil), (*LMemberEmail)(nil), (*LAccountChecked)(nil), (*LEntryChecked)(nil), (*LHolder)(nil), (*LLoan)(nil), (*LPayment)(nil), (*LTransferChecked)(nil)}
	r14 = []any{(*LBranch)(nil), (*LMemberEmail)(nil), (*LAccountParent)(nil), (*LEntryChecked)(nil), (*LHolder)(nil), (*LLoan)(nil), (*LPayment)(nil), (*LTransferChecked)(nil)}
	r15 = []any{(*LBranch)(nil), (*LMemberEmail)(nil), (*LAccountParent)(nil), (*LJournal)(nil), (*LHolder)(nil), (*LLoan)(nil), (*LPaymentJournal)(nil), (*LTransferChecked)(nil)}
	r16 = []any{(*LBranch)(nil), (*LMemberEmail)(nil), (*LAccountParent)(nil), (*LJournalRef)(nil), (*LHolder)(nil), (*LLoan)(nil), (*LPaymentJournal)(nil), (*LTransferChecked)(nil)}
	r17 = []any{(*LBranch)(nil), (*LMemberEmail)(nil), (*LAccountParent)(nil), (*LJournalRef)(nil), (*LHolder)(nil), (*LLoanWide)(nil), (*LPaymentJournal)(nil), (*LTransferChecked)(nil)}
	r18 = []any{(*LBranchNoName)(nil), (*LMemberEmail)(nil), (*LAccountParent)(nil), (*LJournalRef)(nil), (*LHolder)(nil), (*LLoanWide)(nil), (*LPaymentJournal)(nil), (*LTransferChecked)(nil)}
	r19 = []any{(*LBranchNoName)(nil), (*LMemberEmail)(nil), (*LAccountNoMember)(nil), (*LJournalRef)(nil), (*LHolder)(nil), (*LLoanWide)(nil), (*LPaymentJournal)(nil), (*LTransferChecked)(nil)}
	r20 = append(slices.Clone(r19), (*LStatement)(nil))
)

// rejects checks a statement is refused by the schema, inside a rolled-back
// transaction.
func rejects(t *testing.T, s ecsql.ISQLService, why, query string) {
	t.Helper()
	tx, err := s.Client().BeginTx(ctxT(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctxT(t), query); err == nil {
		t.Errorf("%s: accepted %q", why, query)
	}
}

var ledgerReleases = []ledgerRelease{
	{name: "branches and members", models: r1,
		seed: `INSERT INTO branches (code, name) VALUES ('MNL', 'Manila'), ('CEB', 'Cebu'), ('DVO', 'Davao');
INSERT INTO members (name, email) SELECT 'Member ' || g,
  CASE WHEN g % 10 = 0 THEN NULL WHEN g > 45 THEN 'm' || (g - 40) || '@coop.test' ELSE 'm' || g || '@coop.test' END
  FROM generate_series(1, 50) g;`},
	{name: "accounts owned by members", models: r2,
		seed: `INSERT INTO accounts (member_id, number) SELECT id, 'AC' || lpad(id::text, 6, '0') FROM members;`,
		check: func(t *testing.T, s ecsql.ISQLService) {
			rejects(t, s, "account of an unknown member", "INSERT INTO accounts (member_id, number) VALUES (999, 'X')")
			rejects(t, s, "duplicate account number", "INSERT INTO accounts (member_id, number) VALUES (1, 'AC000001')")
		}},
	{name: "members belong to branches (FK on a populated table)", models: r3,
		seed: `UPDATE members SET branch_id = 1 + id % 3;`,
		check: func(t *testing.T, s ecsql.ISQLService) {
			rejects(t, s, "unknown branch", "UPDATE members SET branch_id = 99 WHERE id = 1")
		}},
	{name: "ledger entries", models: r4,
		seed: `INSERT INTO ledger_entries (account_id, amount) SELECT id, 1000 * id FROM accounts UNION ALL SELECT id, 250 FROM accounts;`},
	{name: "indexes declared on the models (expression index included)", models: r5},
	{name: "currency NOT NULL with a string default on a populated table", models: r6,
		check: func(t *testing.T, s ecsql.ISQLService) {
			if n := count(t, s, "SELECT count(*) FROM accounts WHERE currency = 'PHP'"); n != 50 {
				t.Errorf("existing accounts with the default currency = %d, want 50", n)
			}
			rejects(t, s, "account without currency", "UPDATE accounts SET currency = NULL WHERE id = 1")
		}},
	{name: "joint accounts (composite primary key join table)", models: r7},
	{name: "backfill owners into account_holders", models: r7,
		hand: [2]string{`INSERT INTO account_holders (account_id, member_id, role) SELECT id, member_id, 'owner' FROM accounts;`,
			`DELETE FROM account_holders WHERE role = 'owner';`},
		seed: `INSERT INTO account_holders (account_id, member_id, role) SELECT id, member_id + 1, 'joint' FROM accounts WHERE id <= 10;`,
		check: func(t *testing.T, s ecsql.ISQLService) {
			rejects(t, s, "holder twice on one account", "INSERT INTO account_holders (account_id, member_id, role) VALUES (1, 1, 'joint')")
		}},
	{name: "rename members.name to full_name", models: r9,
		check: func(t *testing.T, s ecsql.ISQLService) {
			if n := count(t, s, "SELECT count(*) FROM members WHERE full_name LIKE 'Member %'"); n != 50 {
				t.Errorf("names kept through the rename = %d, want 50", n)
			}
		}},
	{name: "loans and one-to-one loan payments", models: r10,
		seed: `INSERT INTO loans (member_id, account_id, principal, rate_bps, term_months, status)
  SELECT member_id, id, 100000, 1200, 12, 'active' FROM accounts WHERE id <= 10;
WITH e AS (INSERT INTO ledger_entries (account_id, amount) SELECT account_id, -500 FROM loans RETURNING id, account_id)
INSERT INTO loan_payments (loan_id, entry_id, amount) SELECT l.id, e.id, 500 FROM e JOIN loans l ON l.account_id = e.account_id;`,
		check: func(t *testing.T, s ecsql.ISQLService) {
			rejects(t, s, "one entry paying twice", "INSERT INTO loan_payments (loan_id, entry_id, amount) SELECT 1, entry_id, 1 FROM loan_payments LIMIT 1")
		}},
	{name: "transfers (two foreign keys to accounts) referenced by entries", models: r11,
		seed: `WITH t AS (INSERT INTO transfers (from_account_id, to_account_id, amount)
  SELECT g, g + 1, 100 FROM generate_series(1, 20) g RETURNING id, from_account_id, to_account_id, amount)
INSERT INTO ledger_entries (account_id, amount, transfer_id)
  SELECT from_account_id, -amount, id FROM t UNION ALL SELECT to_account_id, amount, id FROM t;`,
		check: func(t *testing.T, s ecsql.ISQLService) {
			rejects(t, s, "transfer from an unknown account", "INSERT INTO transfers (from_account_id, to_account_id, amount) VALUES (999, 1, 5)")
		}},
	{name: "checks and transfer indexes declared on the models", models: r12,
		check: func(t *testing.T, s ecsql.ISQLService) {
			rejects(t, s, "negative balance", "UPDATE accounts SET balance = -1 WHERE id = 1")
			rejects(t, s, "zero entry", "INSERT INTO ledger_entries (account_id, amount) VALUES (1, 0)")
			rejects(t, s, "transfer to itself", "INSERT INTO transfers (from_account_id, to_account_id, amount) VALUES (1, 1, 5)")
		}},
	{name: "email required and unique, though existing data has blanks and duplicates", models: r13,
		fix: [2]string{`UPDATE members SET email = 'member-' || id || '@coop.invalid' WHERE email IS NULL;
UPDATE members m SET email = split_part(email, '@', 1) || '+' || id || '@' || split_part(email, '@', 2)
  WHERE id <> (SELECT min(d.id) FROM members d WHERE d.email = m.email);`, `SELECT 1;`},
		check: func(t *testing.T, s ecsql.ISQLService) {
			if n := count(t, s, "SELECT count(DISTINCT email) FROM members"); n != 50 {
				t.Errorf("distinct emails = %d, want 50", n)
			}
			rejects(t, s, "duplicate email", "UPDATE members SET email = (SELECT email FROM members WHERE id = 2) WHERE id = 1")
		}},
	{name: "sub-accounts (self-reference)", models: r14,
		seed: `INSERT INTO accounts (member_id, number, parent_id) SELECT member_id, number || '-S', id FROM accounts WHERE id <= 5;`,
		check: func(t *testing.T, s ecsql.ISQLService) {
			rejects(t, s, "unknown parent account", "UPDATE accounts SET parent_id = 9999 WHERE id = 1")
		}},
	{name: "rename ledger_entries to journal_entries", models: r15,
		check: func(t *testing.T, s ecsql.ISQLService) {
			// The id sequence must carry on: a new sequence from 1 would collide.
			var before, id int64
			_ = s.Client().NewRaw("SELECT max(id) FROM journal_entries").Scan(ctxT(t), &before)
			if err := s.Client().NewRaw("INSERT INTO journal_entries (account_id, amount) VALUES (1, 7) RETURNING id").Scan(ctxT(t), &id); err != nil || id <= before {
				t.Fatalf("insert after the rename: id %d (max was %d), %v", id, before, err)
			}
		}},
	{name: "idempotency key unique per account (composite unique)", models: r16,
		seed: `INSERT INTO journal_entries (account_id, amount, external_ref) VALUES (1, 3, 'ref-1'), (2, 3, 'ref-1');`,
		check: func(t *testing.T, s ecsql.ISQLService) {
			rejects(t, s, "replayed idempotency key", "INSERT INTO journal_entries (account_id, amount, external_ref) VALUES (1, 3, 'ref-1')")
		}},
	{name: "loan term widened to bigint", models: r17,
		check: func(t *testing.T, s ecsql.ISQLService) {
			if n := count(t, s, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'term_months' AND data_type = 'bigint'"); n != 1 {
				t.Error("term_months is not bigint")
			}
			if n := count(t, s, "SELECT count(*) FROM loans WHERE term_months = 12"); n != 10 {
				t.Errorf("loan terms kept = %d", n)
			}
		}},
	{name: "drop branches.name (reviewed)", models: r18, review: true},
	{name: "drop accounts.member_id now that holders own accounts (reviewed)", models: r19, review: true,
		check: func(t *testing.T, s ecsql.ISQLService) {
			if n := count(t, s, "SELECT count(*) FROM accounts a WHERE parent_id IS NULL AND NOT EXISTS (SELECT 1 FROM account_holders h WHERE h.account_id = a.id AND h.role = 'owner')"); n != 0 {
				t.Errorf("%d accounts lost their owner", n)
			}
		}},
	{name: "monthly statements (composite unique)", models: r20,
		seed: `INSERT INTO statements (account_id, period, closing) SELECT id, '2026-09', balance FROM accounts;`,
		check: func(t *testing.T, s ecsql.ISQLService) {
			rejects(t, s, "two statements for one period", "INSERT INTO statements (account_id, period, closing) VALUES (1, '2026-09', 0)")
		}},
}

// entries is the ledger table's name at the current schema.
func entries(t *testing.T, s ecsql.ISQLService) string {
	if hasTable(t, s, "journal_entries") {
		return "journal_entries"
	}
	if hasTable(t, s, "ledger_entries") {
		return "ledger_entries"
	}
	return ""
}

// rebalance sets every balance to the sum of its entries, as posting does.
func rebalance(t *testing.T, s ecsql.ISQLService) {
	if e := entries(t, s); e != "" {
		execSQL(t, s, "UPDATE accounts a SET balance = (SELECT coalesce(sum(amount), 0) FROM "+e+" e WHERE e.account_id = a.id)")
	}
}

// ledgerInvariants are true after every release: no table lost rows, every
// balance equals its entries, and the declared indexes and checks are there.
func ledgerInvariants(t *testing.T, s ecsql.ISQLService, rows map[string]int, release int) {
	t.Helper()
	for _, tb := range tables(t, s) {
		n := count(t, s, "SELECT count(*) FROM "+tb)
		old := rows[tb]
		if tb == "journal_entries" {
			old = max(old, rows["ledger_entries"])
		}
		if n < old {
			t.Fatalf("release %d: %s lost rows: %d → %d", release, tb, old, n)
		}
		rows[tb] = n
	}
	if e := entries(t, s); e != "" {
		if n := count(t, s, "SELECT count(*) FROM accounts a WHERE balance <> (SELECT coalesce(sum(amount), 0) FROM "+e+" e WHERE e.account_id = a.id)"); n != 0 {
			t.Fatalf("release %d: %d balances disagree with the ledger", release, n)
		}
	}
	want := map[int][]string{
		5:  {"ledger_entries_account_created_idx", "members_lower_email_idx"},
		12: {"accounts_balance_nonneg", "ledger_entries_amount_nonzero", "transfers_amount_positive", "transfers_distinct_accounts", "transfers_from_idx", "transfers_to_idx"},
	}
	for since, names := range want {
		if release < since {
			continue
		}
		for _, name := range names {
			if count(t, s, "SELECT (SELECT count(*) FROM pg_class WHERE relname = ?) + (SELECT count(*) FROM pg_constraint WHERE conname = ?)", name, name) == 0 {
				t.Fatalf("release %d: %s is gone", release, name)
			}
		}
	}
}

// ledgerHistory is a built history: the folder and database it was built
// in, and how many migration files existed after each release.
type ledgerHistory struct {
	env   *env
	files []int
}

// buildLedger runs all twenty releases on a new database with data,
// checking every release, and returns the resulting history.
func buildLedger(t *testing.T) ledgerHistory {
	t.Helper()
	e := newEnv(t)
	ctx := ctxT(t)
	rows := map[string]int{}
	h := ledgerHistory{env: e}
	for i, r := range ledgerReleases {
		n := i + 1
		s := e.started(t, r.models...)
		if r.hand[0] != "" {
			e.write(t, nextFileVersion(e), "hand_"+strings.ReplaceAll(r.name, " ", "_")[:20], r.hand[0], r.hand[1])
		}
		before, snap := e.files("*"), snapshot(t, s)
		err := s.Migrate(ctx)
		if r.fix[0] != "" {
			if !errors.Is(err, ecsql.ErrInvalidMigration) {
				t.Fatalf("release %d (%s): Migrate = %v, want ErrInvalidMigration on the existing data", n, r.name, err)
			}
			if !slices.Equal(e.files("*"), before) || snapshot(t, s) != snap {
				t.Fatalf("release %d: the refused migration left changes", n)
			}
			e.write(t, nextFileVersion(e), "fix_data_for_release", r.fix[0], r.fix[1])
			err = s.Migrate(ctx)
		}
		if err != nil {
			t.Fatalf("release %d (%s): %v", n, r.name, err)
		}
		if review := e.files("*" + ecsql.ReviewSuffix); r.review != (len(review) == 1) {
			t.Fatalf("release %d: review files %v", n, review)
		} else if r.review {
			if snapshot(t, s) != snap {
				t.Fatalf("release %d: a destructive change applied before review", n)
			}
			if err := os.Rename(review[0], strings.TrimSuffix(review[0], ecsql.ReviewSuffix)); err != nil {
				t.Fatal(err)
			}
			if err := s.Migrate(ctx); err != nil {
				t.Fatalf("release %d after review: %v", n, err)
			}
		}
		files := len(e.files("*"))
		if err := s.Migrate(ctx); err != nil || len(e.files("*")) != files {
			t.Fatalf("release %d (%s) does not converge: %v, %v", n, r.name, err, e.files("*"))
		}
		if r.seed != "" {
			execSQL(t, s, r.seed)
			rebalance(t, s)
		}
		ledgerInvariants(t, s, rows, n)
		if r.check != nil {
			r.check(t, s)
		}
		h.files = append(h.files, files)
		_ = s.Stop(ctx)
	}
	return h
}

func TestLedgerFoundation_TwentyReleasesFromModels(t *testing.T) {
	t.Parallel()
	h := buildLedger(t)
	s := h.env.started(t)
	if n := count(t, s, "SELECT count(*) FROM accounts"); n != 55 {
		t.Fatalf("accounts = %d", n)
	}
	if n := count(t, s, "SELECT count(*) FROM journal_entries"); n != 50*2+10+40+1+2 {
		t.Fatalf("journal entries = %d", n)
	}
}

func TestLedgerFoundation_ReplayFromScratchThenRollBackEveryMigration(t *testing.T) {
	t.Parallel()
	h := buildLedger(t)
	want := snapshot(t, h.env.started(t))
	if got := replayAndRollBack(t, h.env); got != want {
		t.Fatalf("replaying the history gives another schema:\n%s", diffLines(want, got))
	}
}

// CI drift check: the migrations alone produce exactly what the models say.
func TestLedgerFoundation_ModelsMatchTheMigrations(t *testing.T) {
	t.Parallel()
	h := buildLedger(t)
	ci := newEnv(t)
	copyMigrations(t, h.env, ci)
	s := ci.started(t, r20...)
	files := len(ci.files("*"))
	if err := s.Migrate(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	if len(ci.files("*")) != files {
		t.Fatalf("models drift from the migrations: %v", ci.files("*_auto.sql*"))
	}
}

// Production stopped at release 6 with its own data, then deploys the other
// fourteen releases at once, including the data fix and both reviewed drops.
func TestLedgerFoundation_ProductionBehindUpgradesInOneDeploy(t *testing.T) {
	t.Parallel()
	h := buildLedger(t)
	prod := newEnv(t)
	copySome(t, h.env, prod, h.files[5])
	p := ecsql.NewSQLService(prod.dsn, 2, 4, prod.dir, true, prod.out, nil, nil)
	ctx := ctxT(t)
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	execSQL(t, p, `INSERT INTO branches (code, name) VALUES ('ILO', 'Iloilo');
INSERT INTO members (name, email, branch_id) SELECT 'Prod ' || g, CASE WHEN g % 4 = 0 THEN NULL ELSE 'p' || (g % 7) || '@prod.test' END, 1 FROM generate_series(1, 30) g;
INSERT INTO accounts (member_id, number) SELECT id, 'PR' || id FROM members;
INSERT INTO ledger_entries (account_id, amount) SELECT id, 5000 + id FROM accounts;`)
	rebalance(t, p)
	money := count(t, p, "SELECT sum(balance)::int FROM accounts")

	copyMigrations(t, h.env, prod)
	if err := p.Run(ctx); err != nil {
		t.Fatalf("deploying fourteen releases: %v", err)
	}
	if n := count(t, p, "SELECT count(*) FROM members"); n != 30 {
		t.Fatalf("members = %d", n)
	}
	if n := count(t, p, "SELECT count(DISTINCT email) FROM members WHERE email IS NOT NULL"); n != 30 {
		t.Fatalf("emails after the data fix = %d distinct", n)
	}
	if got := count(t, p, "SELECT sum(balance)::int FROM accounts"); got != money {
		t.Fatalf("money changed: %d → %d", money, got)
	}
	if n := count(t, p, "SELECT count(*) FROM accounts a WHERE balance <> (SELECT coalesce(sum(amount), 0) FROM journal_entries e WHERE e.account_id = a.id)"); n != 0 {
		t.Fatalf("%d balances disagree with the ledger", n)
	}
	if n := count(t, p, "SELECT count(*) FROM account_holders WHERE role = 'owner'"); n != 30 {
		t.Fatalf("owners backfilled = %d", n)
	}
	if a, b := snapshot(t, h.env.started(t)), snapshot(t, p); a != b {
		t.Fatalf("production schema differs from development:\n%s", diffLines(a, b))
	}
}

// The same models give the same migration files, byte for byte, whatever
// order Go's maps hand Bun the changes in.
func TestLedgerFoundation_GenerationIsDeterministic(t *testing.T) {
	t.Parallel()
	a, b := buildLedger(t), buildLedger(t)
	fa, fb := a.env.files("*"), b.env.files("*")
	if len(fa) != len(fb) {
		t.Fatalf("%d files vs %d", len(fa), len(fb))
	}
	for i := range fa {
		if ca, cb := readFile(t, fa[i]), readFile(t, fb[i]); ca != cb {
			t.Fatalf("file %d differs between two generations:\n%s\n---\n%s", i, ca, cb)
		}
	}
}

// Replicas booting at once each apply the whole history; every migration
// runs once.
func TestLedgerFoundation_ReplicasBootTogether(t *testing.T) {
	t.Parallel()
	h := buildLedger(t)
	fresh := newEnv(t)
	copyMigrations(t, h.env, fresh)
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			s := ecsql.NewSQLService(fresh.dsn, 1, 2, fresh.dir, true, fresh.out, nil, nil)
			ctx := ctxT(t)
			if err := s.Start(ctx); err != nil {
				t.Error(err)
				return
			}
			defer s.Stop(context.Background())
			if err := s.Run(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	s := fresh.started(t)
	if n := count(t, s, "SELECT count(*) FROM goose_db_version WHERE version_id > 0"); n != len(fresh.files("*.sql")) {
		t.Fatalf("applied %d of %d migrations", n, len(fresh.files("*.sql")))
	}
	if a, b := snapshot(t, h.env.started(t)), snapshot(t, s); a != b {
		t.Fatalf("replicas built another schema:\n%s", diffLines(a, b))
	}
}

// A reviewed column drop cannot give the data back: rolling it back on a
// populated table fails (the restored column is NOT NULL with nothing to
// fill it), atomically, and leaves the database as it was.
func TestLedgerFoundation_RollingBackADroppedColumnWithDataFailsCleanly(t *testing.T) {
	t.Parallel()
	h := buildLedger(t)
	s := h.env.started(t)
	ctx := ctxT(t)
	if err := s.Rollback(ctx); err != nil { // statements: no data loss, fine
		t.Fatal(err)
	}
	snap, money := snapshot(t, s), count(t, s, "SELECT sum(balance)::int FROM accounts")
	if err := s.Rollback(ctx); err == nil { // accounts.member_id
		t.Fatal("restored a NOT NULL column over existing rows")
	}
	if snapshot(t, s) != snap || count(t, s, "SELECT sum(balance)::int FROM accounts") != money {
		t.Fatal("the failed rollback changed the database")
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("rolling forward again: %v", err)
	}
}
