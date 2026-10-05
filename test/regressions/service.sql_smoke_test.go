package regressions

import (
	"errors"
	"os"
	"strings"
	"testing"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
)

// Migration smoke tests: the quick checks run right after a deployment to prove the migrated
// database is alive, complete and usable. Each test is either a happy path or a sad path.

// Happy: the deployed schema is complete, current and wired up.
func TestMigrationSmoke_HappyDeployedSchemaIsCompleteAndCurrent(t *testing.T) {
	e := newSQLEnv(t)
	bootstrapBank(t, e)

	app := e.running() // the freshly deployed application instance
	if err := app.Ping(bg); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if app.Client() == nil || app.Client().PingContext(bg) != nil {
		t.Fatal("Client() is not usable after deployment")
	}
	if got := e.countTables(bankTables); got != len(bankTables) {
		t.Fatalf("%d of %d tables exist", got, len(bankTables))
	}
	if v := e.versionOutput(app); v == 0 || v != e.appliedVersion() {
		t.Fatalf("Version reports %d; database says %d", v, e.appliedVersion())
	}
	e.resetStatusOutput()
	if err := app.Status(bg); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(e.statusOutput(), "Pending") {
		t.Fatalf("a migration is still pending after deployment:\n%s", e.statusOutput())
	}
	if n := e.scanInt(`SELECT count(*) FROM pg_constraint WHERE contype = 'c'`); n < 8 {
		t.Fatalf("only %d CHECK constraints; the hardening migration is missing", n)
	}
	if n := e.scanInt(`SELECT count(*) FROM pg_constraint WHERE contype = 'f'`); n < 15 {
		t.Fatalf("only %d foreign keys; relations are missing", n)
	}
	if got := e.triggerCount(); got != 2 {
		t.Fatalf("%d ledger triggers; want 2", got)
	}
}

// Happy: the first real write after deployment, a balanced transfer, succeeds on the migrated schema.
func TestMigrationSmoke_HappyFirstTransferWorks(t *testing.T) {
	e := newSQLEnv(t)
	bootstrapBank(t, e)
	ids := seedBank(t, e, 2, 100_000)

	if err := bankTransfer(bg, e.db, "smoke-1", ids[0], ids[1], 25_000); err != nil {
		t.Fatalf("transfer on the migrated schema: %v", err)
	}
	if got := e.scanInt(`SELECT balance FROM accounts WHERE id = $1`, ids[0]); got != 75_000 {
		t.Fatalf("source balance = %d; want 75000", got)
	}
	if got := e.scanInt(`SELECT balance FROM accounts WHERE id = $1`, ids[1]); got != 125_000 {
		t.Fatalf("destination balance = %d; want 125000", got)
	}
	requireBankInvariants(t, e, 2, 100_000)
}

// Sad: a broken migration in the release stops the deploy, leaves the last good version in place and
// changes nothing, then succeeds once the file is fixed.
func TestMigrationSmoke_SadBrokenReleaseFailsDeployWithoutChange(t *testing.T) {
	e := newSQLEnv(t)
	bootstrapBank(t, e)
	good := e.appliedVersion()

	bad := e.writeNext("bad_release",
		"ALTER TABLE accounts ADD COLUMN release_flag boolean;\nALTER TABLE does_not_exist ADD COLUMN x text;",
		"ALTER TABLE accounts DROP COLUMN release_flag;")

	app := e.newService()
	if err := app.Run(bg); err == nil {
		_ = app.Stop(bg)
		t.Fatal("the deploy succeeded despite a broken migration")
	}
	if err := app.Ping(bg); !errors.Is(err, sqlsvc.ErrNotInitialized) {
		t.Fatalf("Ping after a failed deploy = %v; want ErrNotInitialized", err)
	}
	if e.appliedVersion() != good {
		t.Fatalf("version moved to %d; want it to stay at %d", e.appliedVersion(), good)
	}
	if e.hasColumn("accounts", "release_flag") {
		t.Fatal("the failed migration left a column behind; migrations must be atomic")
	}

	if err := os.WriteFile(bad, []byte("-- +goose Up\nALTER TABLE accounts ADD COLUMN release_flag boolean;\n\n-- +goose Down\nALTER TABLE accounts DROP COLUMN release_flag;\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	app = e.running()
	if !e.hasColumn("accounts", "release_flag") || e.appliedVersion() <= good {
		t.Fatal("the fixed release was not applied")
	}
}

// Sad: when the database disappears, the service reports it instead of pretending to be healthy.
func TestMigrationSmoke_SadDatabaseGoneIsDetected(t *testing.T) {
	e := newSQLEnv(t)
	bootstrapBank(t, e)
	app := e.running()
	if err := app.Ping(bg); err != nil {
		t.Fatalf("Ping before the outage: %v", err)
	}

	e.dropDatabase()

	err := app.Ping(bg)
	if err == nil {
		t.Fatal("Ping succeeded against a dropped database")
	}
	if errors.Is(err, sqlsvc.ErrNotInitialized) {
		t.Fatalf("Ping = %v; want the underlying connection error, not ErrNotInitialized", err)
	}
	if err := app.Migrate(bg); err == nil {
		t.Fatal("Migrate succeeded against a dropped database")
	}
	if err := app.Status(bg); err == nil {
		t.Fatal("Status succeeded against a dropped database")
	}
	fresh := e.newService()
	if err := fresh.Run(bg); err == nil {
		_ = fresh.Stop(bg)
		t.Fatal("a new instance started against a dropped database")
	}
	if err := app.Stop(bg); err != nil {
		t.Logf("Stop after the outage: %v", err)
	}
}

// Happy: redeploying the same release over and over changes nothing and loses nothing.
func TestMigrationSmoke_HappyRepeatedDeploysAreIdempotent(t *testing.T) {
	e := newSQLEnv(t)
	bootstrapBank(t, e)
	ids := seedBank(t, e, 3, 50_000)
	if err := bankTransfer(bg, e.db, "redeploy-1", ids[0], ids[1], 10_000); err != nil {
		t.Fatal(err)
	}
	version, rows, sum, schema := e.appliedVersion(), e.gooseRows(), accountsChecksum(e), e.schema()

	for i := range 3 {
		app := e.newService()
		if err := app.Run(bg); err != nil {
			t.Fatalf("redeploy %d: %v", i+1, err)
		}
		if err := app.Migrate(bg); err != nil {
			t.Fatalf("redeploy %d Migrate: %v", i+1, err)
		}
		if err := app.Stop(bg); err != nil {
			t.Fatal(err)
		}
		if e.appliedVersion() != version || e.gooseRows() != rows {
			t.Fatalf("redeploy %d changed the migration history (version %d, rows %d)", i+1, e.appliedVersion(), e.gooseRows())
		}
	}
	if accountsChecksum(e) != sum || e.schema() != schema {
		t.Fatal("redeploying changed data or schema")
	}
	requireBankInvariants(t, e, 3, 50_000)
}
