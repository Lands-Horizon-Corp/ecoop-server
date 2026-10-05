package regressions

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
)

// These tests need the Postgres from docker-compose.yml:
//
//	docker compose up -d --wait postgres
//
// SQL_TEST_DSN overrides the target. Each test gets its own throwaway database.

type diffWidgetV1 struct {
	bun.BaseModel `bun:"table:widgets"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
}

type diffWidgetV2 struct {
	bun.BaseModel `bun:"table:widgets"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	Email         string `bun:"email"`
}

const defaultPostgresDSN = "postgres://ecoop:ecoop-test-pass@localhost:5432/ecoop_test?sslmode=disable"

// newDiffService creates an empty database and a running SQLService against it, with the
// working directory moved to a temp dir so migrations are written there.
func newDiffService(t *testing.T) (svc sqlsvc.SQLServices, migrations string) {
	t.Helper()
	adminDSN := envOr("SQL_TEST_DSN", defaultPostgresDSN)
	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("SQL_TEST_DSN: %v", err)
	}
	requireReachable(t, u.Host)

	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	dbName := fmt.Sprintf("regress_%d", time.Now().UnixNano())
	if _, err := admin.Exec(`CREATE DATABASE ` + dbName); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(`DROP DATABASE IF EXISTS ` + dbName + ` WITH (FORCE)`) })

	u.Path = "/" + dbName
	dsn := u.String()

	t.Chdir(t.TempDir())
	migrations = filepath.Join("src", "database", "migrations")
	if err := os.MkdirAll(migrations, 0o755); err != nil {
		t.Fatal(err)
	}

	svc = sqlsvc.NewSQLService(dsn, 2, 5, nil)
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc, migrations
}

func tableHasColumn(t *testing.T, db *bun.DB, table, column string) bool {
	t.Helper()
	var n int
	err := db.NewRaw(
		`SELECT count(*) FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
		table, column,
	).Scan(context.Background(), &n)
	if err != nil {
		t.Fatalf("inspect columns: %v", err)
	}
	return n > 0
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestSQLDiff_ModelChangesBecomeGooseMigrations(t *testing.T) {
	ctx := context.Background()
	svc, dir := newDiffService(t)
	db := svc.Client()

	// 1. A new model becomes a CREATE TABLE migration with a matching DROP TABLE down.
	path, err := svc.Diff(ctx, "Create Widgets!", (*diffWidgetV1)(nil))
	if err != nil {
		t.Fatalf("Diff(new model): %v", err)
	}
	if filepath.Dir(path) != dir || !strings.HasSuffix(path, "_create_widgets.sql") {
		t.Fatalf("migration path = %q; want a timestamped create_widgets.sql in %s", path, dir)
	}
	content := readFile(t, path)
	up, down, _ := strings.Cut(content, "-- +goose Down")
	if !strings.HasPrefix(up, "-- +goose Up") || !strings.Contains(up, "CREATE TABLE") || !strings.Contains(down, "DROP TABLE") {
		t.Fatalf("migration is not goose Up/Down with CREATE/DROP TABLE:\n%s", content)
	}

	if err := svc.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v\n%s", err, content)
	}
	if !tableHasColumn(t, db, "widgets", "name") {
		t.Fatal("widgets table was not created by the generated migration")
	}

	// 2. Applied and in sync: nothing more to generate.
	if again, err := svc.Diff(ctx, "noop", (*diffWidgetV1)(nil)); err != nil || again != "" {
		t.Fatalf("Diff when in sync = %q, %v; want no file", again, err)
	}

	// 3. Changing the struct becomes ALTER TABLE ADD COLUMN, reversible.
	time.Sleep(1100 * time.Millisecond) // goose versions are second-resolution timestamps
	path2, err := svc.Diff(ctx, "add widget email", (*diffWidgetV2)(nil))
	if err != nil || path2 == "" {
		t.Fatalf("Diff(added column) = %q, %v", path2, err)
	}
	content2 := readFile(t, path2)
	if !strings.Contains(content2, "ADD COLUMN") || !strings.Contains(content2, "DROP COLUMN") {
		t.Fatalf("expected ADD COLUMN up and DROP COLUMN down:\n%s", content2)
	}
	if err := svc.Migrate(ctx); err != nil {
		t.Fatalf("Migrate(add column): %v\n%s", err, content2)
	}
	if !tableHasColumn(t, db, "widgets", "email") {
		t.Fatal("email column missing after migrating")
	}

	// 4. Down really undoes it, and Up redoes it.
	if err := svc.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if tableHasColumn(t, db, "widgets", "email") {
		t.Fatal("email column still present after rollback")
	}
	if err := svc.Migrate(ctx); err != nil {
		t.Fatalf("re-Migrate: %v", err)
	}
	if !tableHasColumn(t, db, "widgets", "email") {
		t.Fatal("email column missing after re-applying")
	}
}

func TestSQLDiff_RefusesWhileMigrationsArePending(t *testing.T) {
	ctx := context.Background()
	svc, _ := newDiffService(t)

	if _, err := svc.Diff(ctx, "first", (*diffWidgetV1)(nil)); err != nil {
		t.Fatalf("first Diff: %v", err)
	}
	// The first migration was written but not applied, so a second diff would repeat it.
	if _, err := svc.Diff(ctx, "second", (*diffWidgetV1)(nil)); !errors.Is(err, sqlsvc.ErrPendingMigrations) {
		t.Fatalf("Diff with a pending migration = %v; want ErrPendingMigrations", err)
	}
}

func TestSQLDiff_RejectsBadInput(t *testing.T) {
	ctx := context.Background()
	svc, dir := newDiffService(t)

	if _, err := svc.Diff(ctx, "x"); !errors.Is(err, sqlsvc.ErrNoModels) {
		t.Errorf("no models = %v; want ErrNoModels", err)
	}
	for _, name := range []string{"", "   ", "!!!", "../../etc"} {
		path, err := svc.Diff(ctx, name, (*diffWidgetV1)(nil))
		if name == "../../etc" {
			// Path separators are flattened into the file name, never followed.
			if err != nil || filepath.Dir(path) != dir {
				t.Errorf("Diff(%q) = %q, %v; want a file inside %s", name, path, err, dir)
			}
			continue
		}
		if !errors.Is(err, sqlsvc.ErrInvalidName) {
			t.Errorf("Diff(%q) = %v; want ErrInvalidName", name, err)
		}
	}

	notRunning := sqlsvc.NewSQLService("postgres://invalid", 1, 1, nil)
	if _, err := notRunning.Diff(ctx, "x", (*diffWidgetV1)(nil)); !errors.Is(err, sqlsvc.ErrNotInitialized) {
		t.Errorf("Diff before Run = %v; want ErrNotInitialized", err)
	}
}
