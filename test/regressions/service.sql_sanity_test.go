package regressions

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
)

// Migration sanity tests: the pre-deployment (CI) checks that prove the migration files are valid,
// reversible and consistent with the models before anything reaches a real database.

// Happy: every migration reverses cleanly, one at a time and all at once, and replays to the same schema.
func TestMigrationSanity_HappyEveryMigrationIsReversible(t *testing.T) {
	e := newSQLEnv(t)
	svc := bootstrapBank(t, e)
	built := e.schema()
	steps := len(e.migrationFiles())

	for i := range steps {
		if err := svc.Rollback(bg); err != nil {
			t.Fatalf("Rollback #%d of %d: %v", i+1, steps, err)
		}
	}
	if e.schema() != "" || e.triggerCount() != 0 {
		t.Fatalf("rolling back every migration left schema or triggers behind:\n%s", e.schema())
	}
	if n := e.scanInt(`SELECT count(*) FROM pg_proc WHERE proname IN ('ledger_immutable', 'ledger_balanced')`); n != 0 {
		t.Fatalf("%d trigger functions survived the rollback", n)
	}

	if err := svc.UpSteps(bg, steps); err != nil {
		t.Fatalf("UpSteps(%d): %v", steps, err)
	}
	if e.schema() != built || e.triggerCount() != 2 {
		t.Fatal("replaying the migrations did not rebuild the same schema")
	}

	if err := svc.RollbackTo(bg, 0); err != nil {
		t.Fatalf("RollbackTo(0): %v", err)
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("Migrate after RollbackTo(0): %v", err)
	}
	if e.schema() != built {
		t.Fatal("RollbackTo(0) then Migrate did not rebuild the same schema")
	}
}

// Happy: the migrations produce exactly the schema the models describe, so CI sees no drift.
func TestMigrationSanity_HappyModelsMatchMigratedSchema(t *testing.T) {
	e := newSQLEnv(t)
	svc := bootstrapBank(t, e)
	requireConverged(t, e, svc, bankModels()...)
}

// Sad: two migrations sharing a version are refused before anything runs.
func TestMigrationSanity_SadDuplicateVersionsAreRejected(t *testing.T) {
	e := newSQLEnv(t)
	e.write(7, "first", `CREATE TABLE first_t (id int);`, `DROP TABLE first_t;`)
	e.write(7, "second", `CREATE TABLE second_t (id int);`, `DROP TABLE second_t;`)

	svc := e.newService()
	if err := svc.Run(bg); err == nil {
		_ = svc.Stop(bg)
		t.Fatal("Run accepted two migrations with the same version")
	}
	if e.hasTable("first_t") || e.hasTable("second_t") || e.appliedVersion() != 0 {
		t.Fatal("a migration ran even though the set was invalid")
	}
}

// Sad: malformed migration files fail loudly and leave the database untouched.
func TestMigrationSanity_SadMalformedMigrationsAreRejected(t *testing.T) {
	cases := map[string]string{
		"syntax error":             "-- +goose Up\nCREAT TABLE broken (id int);\n\n-- +goose Down\nDROP TABLE broken;\n",
		"no goose annotations":     "CREATE TABLE plain (id int);\n",
		"unterminated block":       "-- +goose Up\n-- +goose StatementBegin\nCREATE TABLE open_t (id int);\n",
		"statement after the down": "-- +goose Up\nCREATE TABLE ok_t (id int);\n\n-- +goose Down\nDROP TABLE missing_t;\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			e := newSQLEnv(t)
			if err := os.WriteFile(filepath.Join(e.dir, "00001_candidate.sql"), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}

			svc := e.newService()
			err := svc.Run(bg)
			if name == "statement after the down" {
				// Valid to apply; the broken Down only shows up when it is rolled back.
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				defer func() { _ = svc.Stop(bg) }()
				if err := svc.Rollback(bg); err == nil {
					t.Fatal("Rollback succeeded with a Down that drops a missing table")
				}
				if !e.hasTable("ok_t") {
					t.Fatal("a failed Down must leave the table in place")
				}
				return
			}
			if err == nil {
				_ = svc.Stop(bg)
				t.Fatalf("Run accepted a malformed migration (%s)", name)
			}
			if e.appliedVersion() != 0 || e.countTables([]string{"broken", "plain", "open_t"}) != 0 {
				t.Fatal("a malformed migration changed the database")
			}
		})
	}
}

// Sad: drift and unapplied files are both caught before they can reach production.
func TestMigrationSanity_SadDriftAndPendingMigrationsAreCaught(t *testing.T) {
	e := newSQLEnv(t)
	svc := bootstrapBank(t, e)

	e.exec(`ALTER TABLE customers ADD COLUMN sneaky text`) // a hotfix applied by hand, never migrated
	path, err := svc.Diff(bg, "catch drift", bankModels()...)
	if err != nil || path == "" {
		t.Fatalf("Diff did not report the drift: %q, %v", path, err)
	}
	content := readFile(t, path)
	if !strings.Contains(gooseUp(content), "DROP COLUMN") || !strings.Contains(gooseUp(content), "sneaky") {
		t.Fatalf("the drift migration should remove the manual column:\n%s", content)
	}

	// The generated file is unapplied, so a second diff must refuse rather than stack duplicates.
	if _, err := svc.Diff(bg, "again", bankModels()...); !errors.Is(err, sqlsvc.ErrPendingMigrations) {
		t.Fatalf("Diff with a pending migration = %v; want ErrPendingMigrations", err)
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatal(err)
	}
	requireConverged(t, e, svc, bankModels()...)
}
