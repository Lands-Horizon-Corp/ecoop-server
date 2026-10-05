package regressions

import (
	"errors"
	"testing"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/uptrace/bun"
)

// bun's auto-migrator cannot express every Postgres schema. These tests pin the contract that
// matters in production: for anything it cannot do correctly, Diff must fail loudly and write
// nothing, never save a migration that fails (or half-applies) later.

type dSettingsV1 struct {
	bun.BaseModel `bun:"table:settings"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
}

type dSettingsV2 struct { // adds a NOT NULL column with a string default
	bun.BaseModel `bun:"table:settings"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	Mode          string `bun:"mode,notnull,default:'standard'"`
}

type dMoney struct { // numeric with precision and scale, as money columns usually are
	bun.BaseModel `bun:"table:money"`
	ID            int64   `bun:"id,pk,autoincrement"`
	Amount        float64 `bun:"amount,notnull,type:numeric(18,2)"`
}

// requireSafeOutcome accepts either a clean refusal (ErrInvalidMigration or a diff error with no
// file written) or a migration that applies and then converges; it never accepts a broken file.
func requireSafeOutcome(t *testing.T, e *sqlEnv, svc sqlsvc.SQLServices, name string, models ...any) {
	t.Helper()
	filesBefore := len(e.migrationFiles())
	path, err := svc.Diff(bg, name, models...)
	if err != nil {
		if len(e.migrationFiles()) != filesBefore {
			t.Fatalf("Diff failed (%v) but still wrote a migration file", err)
		}
		t.Logf("Diff refused as designed: %v", err)
		return
	}
	if path == "" {
		return
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("Diff saved a migration that does not apply: %v\n%s", err, readFile(t, path))
	}
	if err := svc.Rollback(bg); err != nil {
		t.Fatalf("Diff saved a migration whose Down does not apply: %v\n%s", err, readFile(t, path))
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatalf("re-applying the saved migration failed: %v", err)
	}
	requireConverged(t, e, svc, models...)
}

func TestSQLDiff_StringDefaultOnExistingTableIsSafe(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.running()
	diffApply(t, e, svc, "settings", (*dSettingsV1)(nil))
	e.exec(`INSERT INTO settings (name) VALUES ('a'), ('b')`)

	requireSafeOutcome(t, e, svc, "add mode", (*dSettingsV2)(nil))

	if e.scanInt(`SELECT count(*) FROM settings`) != 2 {
		t.Fatal("existing rows were lost")
	}
}

func TestSQLDiff_NumericWithPrecisionIsSafe(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.running()
	requireSafeOutcome(t, e, svc, "money", (*dMoney)(nil))
}

func TestSQLDiff_InvalidMigrationErrorIsDistinguishable(t *testing.T) {
	e := newSQLEnv(t)
	svc := e.running()
	// A hand-made drift the diff will try to revert, combined with a table that has a dependent view:
	// dropping the column fails, which must surface as ErrInvalidMigration rather than a saved file.
	diffApply(t, e, svc, "settings", (*dSettingsV1)(nil))
	e.exec(`ALTER TABLE settings ADD COLUMN extra text`)
	e.exec(`CREATE VIEW settings_view AS SELECT id, extra FROM settings`)

	filesBefore := len(e.migrationFiles())
	_, err := svc.Diff(bg, "revert drift", (*dSettingsV1)(nil))
	if !errors.Is(err, sqlsvc.ErrInvalidMigration) {
		t.Fatalf("Diff = %v; want ErrInvalidMigration (DROP COLUMN is blocked by a dependent view)", err)
	}
	if len(e.migrationFiles()) != filesBefore {
		t.Fatal("an invalid migration was written to disk")
	}
	// The dry run must leave the database exactly as it was.
	if !e.hasColumn("settings", "extra") {
		t.Fatal("validation changed the database; it must always roll back")
	}
}
