//go:build integration

package sql_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// Release safety: checks before merge (sanity), after deploy (smoke), and
// for canary rollouts, on the banking history.

// rowsDigest summarises the banking data, to compare before and after.
func rowsDigest(t testing.TB, s ecsql.SQLServices) string {
	t.Helper()
	var d string
	err := s.Client().NewRaw(`SELECT md5(
  (SELECT coalesce(string_agg(id || ':' || email, ',' ORDER BY id), '') FROM customers) ||
  (SELECT coalesce(string_agg(id || ':' || number || ':' || balance_cents || ':' || status, ',' ORDER BY id), '') FROM accounts) ||
  (SELECT coalesce(string_agg(id || ':' || amount_cents, ',' ORDER BY id), '') FROM ledger))`).Scan(ctxT(t), &d)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// Pre-merge sanity.

func TestMigrationSanity_HappyEveryMigrationIsReversible(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, len(bankReleases))
	final := replayAndRollBack(t, e) // one at a time
	s := e.started(t)
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.RollbackTo(ctx, 0); err != nil { // all at once
		t.Fatal(err)
	}
	if got := tables(t, s); len(got) != 0 {
		t.Fatalf("tables after rolling everything back = %v", got)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if got := snapshot(t, s); got != final {
		t.Fatalf("replay after full rollback differs:\n%s", diffLines(final, got))
	}
}

func TestMigrationSanity_HappyModelsMatchMigratedSchema(t *testing.T) {
	t.Parallel()
	dev := newEnv(t)
	evolve(t, dev, 0, len(evoReleases))
	ci := newEnv(t)
	copyMigrations(t, dev, ci)
	s := ci.started(t, finalEvoModels...)
	files := len(ci.files("*"))
	if err := s.Migrate(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	if got := len(ci.files("*")); got != files {
		t.Fatalf("CI sees drift between the migrations and the models: %v", ci.files("*_auto*"))
	}
}

func TestMigrationSanity_SadDuplicateVersionsAreRejected(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, 5)
	e.write(t, 30, "also_thirty", "CREATE TABLE dup (id int);", "DROP TABLE dup;")
	s := e.started(t)
	if err := s.Migrate(ctxT(t)); err == nil {
		t.Fatal("duplicate versions accepted")
	}
	if got := tables(t, s); len(got) != 0 {
		t.Fatalf("something ran: %v", got)
	}
}

func TestMigrationSanity_SadMalformedMigrationsAreRejected(t *testing.T) {
	t.Parallel()
	for name, up := range map[string]string{
		"syntax error":        "CREATE TABLE broken (id int",
		"unterminated block":  "-- +goose StatementBegin\nCREATE TABLE b (id int);",
		"unknown column":      "UPDATE accounts SET nope = 1;",
		"function without $$": "-- +goose StatementBegin\nCREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT';\n-- +goose StatementEnd",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e, s := bankAt(t, len(bankReleases))
			snap, data := snapshot(t, s), rowsDigest(t, s)
			e.write(t, 210, "broken", up, "SELECT 1;")
			if err := s.Migrate(ctxT(t)); err == nil {
				t.Fatal("malformed migration accepted")
			}
			if snapshot(t, s) != snap || rowsDigest(t, s) != data {
				t.Fatal("malformed migration changed the database")
			}
			if v := version(t, s, e); v != "200" {
				t.Fatalf("version = %s", v)
			}
		})
	}
}

func TestMigrationSanity_SadDriftAndPendingMigrationsAreCaught(t *testing.T) {
	t.Parallel()
	// Pending: Status shows the file as pending.
	e := newEnv(t)
	writeReleases(t, e, 1, len(bankReleases))
	s := e.started(t)
	ctx := ctxT(t)
	if err := s.UpSteps(ctx, 19); err != nil {
		t.Fatal(err)
	}
	e.out.Reset()
	if err := s.Status(ctx); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(e.out.String(), "pending") || !strings.Contains(e.out.String(), "200_customer_accounts_view.sql") {
		t.Fatalf("pending migration not reported:\n%s", e.out.String())
	}
	// Drift: a hand-made change is reported, not planned.
	d := newEnv(t)
	m := d.started(t, (*EvoV2)(nil))
	if err := m.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	execSQL(t, m, "ALTER TABLE evo ALTER COLUMN name DROP NOT NULL")
	if err := m.Migrate(ctx); !errors.Is(err, ecsql.ErrDrift) {
		t.Fatalf("drift not caught: %v", err)
	}
}

// Post-deploy smoke.

func TestMigrationSmoke_HappyDeployedSchemaIsCompleteAndCurrent(t *testing.T) {
	t.Parallel()
	e, s := bankAt(t, len(bankReleases))
	if v := version(t, s, e); v != "200" {
		t.Fatalf("version = %s, want the newest file's", v)
	}
	e.out.Reset()
	if err := s.Status(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(e.out.String(), "pending") {
		t.Fatalf("pending after deploy:\n%s", e.out.String())
	}
	for _, q := range []string{
		"SELECT count(*) FROM pg_views WHERE viewname IN ('account_balances', 'customer_accounts')",
		"SELECT count(*) FROM pg_trigger WHERE tgname = 'ledger_no_change'",
		"SELECT count(*) FROM pg_proc WHERE proname = 'transfer'",
	} {
		if count(t, s, q) == 0 {
			t.Errorf("missing: %s", q)
		}
	}
	// The database itself: UTF-8 and a supported server.
	var enc string
	var ver int
	if err := s.Client().NewRaw("SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database()").Scan(ctxT(t), &enc); err != nil || enc != "UTF8" {
		t.Errorf("encoding = %q, %v", enc, err)
	}
	if err := s.Client().NewRaw("SELECT current_setting('server_version_num')::int").Scan(ctxT(t), &ver); err != nil || ver < 140000 {
		t.Errorf("server version = %d, %v", ver, err)
	}
}

func TestMigrationSmoke_HappyFirstTransferWorks(t *testing.T) {
	t.Parallel()
	_, s := bankAt(t, len(bankReleases))
	a, b := account(t, s, 10_000), account(t, s, 0)
	if _, err := s.Client().ExecContext(ctxT(t), "SELECT transfer(?, ?, 2500)", a, b); err != nil {
		t.Fatal(err)
	}
	if balance(t, s, a) != 7500 || balance(t, s, b) != 2500 {
		t.Fatal("first transfer unbalanced")
	}
}

func TestMigrationSmoke_SadBrokenReleaseFailsDeployWithoutChange(t *testing.T) {
	t.Parallel()
	e, s := bankAt(t, len(bankReleases))
	snap, data := snapshot(t, s), rowsDigest(t, s)
	e.write(t, 210, "broken", "ALTER TABLE accounts ADD COLUMN branch text;\nUPDATE accounts SET branch = nope;", "ALTER TABLE accounts DROP COLUMN branch;")
	deploy := ecsql.NewSQLService(e.dsn, 1, 2, e.dir, true, e.out, nil, nil)
	ctx := ctxT(t)
	if err := deploy.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer deploy.Stop(context.Background())
	if err := deploy.Run(ctx); err == nil {
		t.Fatal("broken release deployed")
	}
	if v := version(t, s, e); v != "200" || snapshot(t, s) != snap || rowsDigest(t, s) != data {
		t.Fatalf("broken deploy changed the database (version %s)", v)
	}
	e.write(t, 210, "broken", "ALTER TABLE accounts ADD COLUMN branch text;\nUPDATE accounts SET branch = 'main';", "ALTER TABLE accounts DROP COLUMN branch;")
	if err := deploy.Run(ctx); err != nil {
		t.Fatalf("fixed release: %v", err)
	}
	if v := version(t, s, e); v != "210" {
		t.Fatalf("version = %s", v)
	}
}

func TestMigrationSmoke_SadDatabaseGoneIsDetected(t *testing.T) {
	t.Parallel()
	e, p := newEnv(t), newProxy(t)
	writeReleases(t, e, 1, 3)
	s := proxied(t, e, p)
	if err := s.Migrate(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	_ = p.ln.Close()
	p.DropAll()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Ping(ctx); err == nil {
		t.Fatal("Ping reports healthy with the database gone")
	}
	if err := s.Version(ctx); err == nil {
		t.Fatal("Version reports a version with the database gone")
	}
	if err := s.Migrate(ctx); err == nil || errors.Is(err, ecsql.ErrNotStarted) {
		t.Fatalf("Migrate = %v, want a connection error", err)
	}
}

func TestMigrationSmoke_HappyRepeatedDeploysAreIdempotent(t *testing.T) {
	t.Parallel()
	e, s := bankAt(t, len(bankReleases))
	snap, data := snapshot(t, s), rowsDigest(t, s)
	for i := range 5 {
		d := ecsql.NewSQLService(e.dsn, 1, 2, e.dir, true, e.out, nil, nil)
		ctx := ctxT(t)
		if err := d.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := d.Run(ctx); err != nil {
			t.Fatalf("deploy %d: %v", i, err)
		}
		_ = d.Stop(ctx)
	}
	if snapshot(t, s) != snap || rowsDigest(t, s) != data || version(t, s, e) != "200" {
		t.Fatal("repeated deploys changed something")
	}
}

// Canary rollouts: the canary database takes a release first, then the
// fleet.

func TestMigrationCanary_HappyCanaryThenFleetConverge(t *testing.T) {
	t.Parallel()
	_, canary := bankAt(t, 19)
	data := rowsDigest(t, canary)
	if err := canary.Migrate(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	if rowsDigest(t, canary) != data {
		t.Fatal("canary lost data")
	}
	want := snapshot(t, canary)
	for i := range 2 {
		_, node := bankAt(t, 19)
		if err := node.Migrate(ctxT(t)); err != nil {
			t.Fatal(err)
		}
		if got := snapshot(t, node); got != want {
			t.Fatalf("fleet node %d differs from the canary:\n%s", i, diffLines(want, got))
		}
	}
}

func TestMigrationCanary_SadDataDependentFailureStopsAtCanary(t *testing.T) {
	t.Parallel()
	// Valid on clean data, fails on the canary's real data (duplicate names).
	release := []string{"CREATE UNIQUE INDEX customers_full_name_key ON customers (full_name);", "DROP INDEX customers_full_name_key;"}
	clean := newEnv(t)
	writeReleases(t, clean, 1, len(bankReleases))
	clean.write(t, 210, "unique_name", release[0], release[1])
	cs := clean.started(t)
	if err := cs.Migrate(ctxT(t)); err != nil {
		t.Fatalf("release fails on clean data: %v", err)
	}
	ce, canary := bankAt(t, len(bankReleases))
	execSQL(t, canary, "UPDATE customers SET full_name = 'Same' WHERE id <= 2")
	ce.write(t, 210, "unique_name", release[0], release[1])
	snap := snapshot(t, canary)
	if err := canary.Migrate(ctxT(t)); err == nil {
		t.Fatal("release applied over duplicate data")
	}
	if v := version(t, canary, ce); v != "200" || snapshot(t, canary) != snap {
		t.Fatal("the failed canary changed")
	}
}

func TestMigrationCanary_HappyRollbackRestoresSchemaAndData(t *testing.T) {
	t.Parallel()
	e, s := bankAt(t, len(bankReleases))
	snap, data := snapshot(t, s), rowsDigest(t, s)
	e.write(t, 210, "branch", "ALTER TABLE accounts ADD COLUMN branch text NOT NULL DEFAULT 'main';\nCREATE INDEX accounts_branch_idx ON accounts (branch);",
		"DROP INDEX accounts_branch_idx;\nALTER TABLE accounts DROP COLUMN branch;")
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if got := snapshot(t, s); got != snap {
		t.Fatalf("rollback did not restore the schema:\n%s", diffLines(snap, got))
	}
	if rowsDigest(t, s) != data {
		t.Fatal("rollback did not keep the data")
	}
	if err := s.Migrate(ctx); err != nil || version(t, s, e) != "210" {
		t.Fatalf("re-release: %v", err)
	}
}

func TestMigrationCanary_SadLockContentionTimesOutWithoutChange(t *testing.T) {
	t.Parallel()
	e, s := bankAt(t, len(bankReleases))
	ctx := ctxT(t)
	tx, err := s.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "SELECT * FROM accounts FOR UPDATE"); err != nil { // a long-running transaction
		t.Fatal(err)
	}
	e.write(t, 210, "branch", "ALTER TABLE accounts ADD COLUMN branch text;", "ALTER TABLE accounts DROP COLUMN branch;")
	snap := snapshot(t, s)
	short, cancel := context.WithTimeout(ctx, time.Second)
	err = s.Migrate(short)
	cancel()
	if err == nil {
		t.Fatal("migration ran through the blocker")
	}
	_ = tx.Rollback()
	if v := version(t, s, e); v != "200" || snapshot(t, s) != snap {
		t.Fatal("timed-out migration changed the database")
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("after the blocker left: %v", err)
	}
}

func TestMigrationCanary_HappyStagedRolloutAcrossTenants(t *testing.T) {
	t.Parallel()
	var want string
	for i, name := range []string{"canary", "tenant-a", "tenant-b"} {
		e, s := bankAt(t, 17)
		if err := s.Migrate(ctxT(t)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if v := version(t, s, e); v != "200" {
			t.Fatalf("%s: version %s", name, v)
		}
		got := snapshot(t, s)
		if i == 0 {
			want = got
		} else if got != want {
			t.Fatalf("%s differs from the canary:\n%s", name, diffLines(want, got))
		}
	}
}
