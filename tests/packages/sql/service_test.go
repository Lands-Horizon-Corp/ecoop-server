package sql_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// Core service behaviour: start, stop, migrate, roll back, status, create,
// atomic failure, concurrent startup.

func TestSQLService_Lifecycle(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.service()
	ctx := ctxT(t)
	if err := s.Ping(ctx); !errors.Is(err, ecsql.ErrNotStarted) {
		t.Fatalf("Ping before Start = %v", err)
	}
	for i := range 3 {
		if err := s.Start(ctx); err != nil {
			t.Fatalf("Start %d: %v", i, err)
		}
		if err := s.Ping(ctx); err != nil {
			t.Fatalf("Ping %d: %v", i, err)
		}
		if err := s.Stop(ctx); err != nil {
			t.Fatalf("Stop %d: %v", i, err)
		}
		if err := s.Ping(ctx); !errors.Is(err, ecsql.ErrNotStarted) {
			t.Fatalf("Ping after Stop %d = %v", i, err)
		}
	}
	unreachable := ecsql.NewSQLService("postgres://u:p@127.0.0.1:1/db?sslmode=disable", 1, 1, nil, false, nil, nil, nil)
	if err := unreachable.Start(ctxShort(t)); err == nil {
		t.Fatal("unreachable database accepted")
	}
	malformed := ecsql.NewSQLService("::not a dsn::", 1, 1, nil, false, nil, nil, nil)
	if err := malformed.Start(ctx); !errors.Is(err, ecsql.ErrInvalidDSN) {
		t.Fatalf("malformed DSN = %v, want ErrInvalidDSN", err)
	}
}

func TestSQLService_RunAppliesPendingMigrations(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, 5)
	s := ecsql.NewSQLService(e.dsn, 1, 2, e.dir, true, e.out, nil, nil)
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if v := version(t, s, e); v != "50" {
		t.Fatalf("version = %s, want 50", v)
	}
}

func TestSQLService_WithAutoMigrateDisabledLeavesMigrationsPending(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, 1)
	s := e.started(t) // autoMigrate off
	ctx := ctxT(t)
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	e.out.Reset()
	if err := s.Status(ctx); err != nil || !strings.Contains(e.out.String(), "pending") {
		t.Fatalf("Status = %v:\n%s", err, e.out.String())
	}
	if err := s.Migrate(ctx); err != nil || version(t, s, e) != "10" {
		t.Fatalf("explicit Migrate: %v", err)
	}
}

func TestSQLService_RunWithNoMigrationsIsFine(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := ecsql.NewSQLService(e.dsn, 1, 2, e.dir, true, e.out, nil, nil)
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	for name, fn := range map[string]func() error{
		"Run": func() error { return s.Run(ctx) }, "Migrate": func() error { return s.Migrate(ctx) },
		"Status": func() error { return s.Status(ctx) }, "Fresh": func() error { return s.Fresh(ctx) },
		"Rollback": func() error { return s.Rollback(ctx) }, "Redo": func() error { return s.Redo(ctx) },
	} {
		if err := fn(); err != nil {
			t.Errorf("%s with no migrations: %v", name, err)
		}
	}
	if v := version(t, s, e); v != "0" {
		t.Fatalf("version = %s", v)
	}
	e.out.Reset()
	_ = s.Status(ctx)
	if got := strings.TrimSpace(e.out.String()); !strings.HasPrefix(got, "VERSION") || strings.Contains(got, "\n") {
		t.Fatalf("Status with no migrations:\n%s", got)
	}
}

// A migrations folder that disappears after setup is reported, not a crash.
// It is an error rather than "no migrations": a deploy that silently skips
// its migrations would run new code on an old schema.
func TestSQLService_RunWithMissingMigrationsDirectory(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := ecsql.NewSQLService(e.dsn, 1, 2, e.dir, true, e.out, nil, nil)
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	_ = os.RemoveAll(e.path)
	var err error
	mustNotPanic(t, "Run", func() { err = s.Run(ctx) })
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Run = %v, want a not-exist error", err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSQLService_WithoutMigrationsDirectoryOnlyConnects(t *testing.T) {
	t.Parallel()
	dsn := newDatabase(t)
	s := ecsql.NewSQLService(dsn, 1, 2, nil, false, nil, nil, nil)
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); !errors.Is(err, ecsql.ErrNoMigrationsDir) {
		t.Fatalf("Migrate = %v", err)
	}
}

func TestSQLService_MigrationsPathMustBeADirectory(t *testing.T) {
	t.Parallel()
	dsn := newDatabase(t)
	file := filepath.Join(t.TempDir(), "migrations.sql")
	_ = os.WriteFile(file, nil, 0o644)
	f, _ := os.Open(file)
	defer f.Close()
	s := ecsql.NewSQLService(dsn, 1, 2, f, true, nil, nil, nil)
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	if err := s.Run(ctx); !errors.Is(err, ecsql.ErrNotADirectory) {
		t.Fatalf("Run = %v, want ErrNotADirectory", err)
	}
}

func TestSQLService_MigrateAndRollbackOperations(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, 5)
	s := e.started(t)
	ctx := ctxT(t)
	at := func(want string) {
		t.Helper()
		if v := version(t, s, e); v != want {
			t.Fatalf("version = %s, want %s", v, want)
		}
	}
	for _, n := range []int{0, -1} {
		if err := s.UpSteps(ctx, n); !errors.Is(err, ecsql.ErrInvalidSteps) {
			t.Fatalf("UpSteps(%d) = %v", n, err)
		}
	}
	if err := s.UpSteps(ctx, 6); !errors.Is(err, ecsql.ErrTooManySteps) {
		t.Fatalf("UpSteps(6) = %v", err)
	}
	at("0")
	_ = s.UpSteps(ctx, 2)
	at("20")
	for range 2 {
		_ = s.Migrate(ctx)
		at("50")
	}
	_ = s.Rollback(ctx)
	at("40")
	if err := s.RollbackSteps(ctx, 5); !errors.Is(err, ecsql.ErrTooManySteps) {
		t.Fatalf("RollbackSteps(5) = %v", err)
	}
	at("40")
	_ = s.RollbackSteps(ctx, 2)
	at("20")
	_ = s.Migrate(ctx)
	_ = s.RollbackTo(ctx, 30)
	at("30")
	_ = s.Redo(ctx)
	at("30")
	if err := s.Fresh(ctx); err != nil {
		t.Fatal(err)
	}
	at("50")
}

func TestSQLService_StatusAndVersionOutput(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, 3)
	s := e.started(t)
	ctx := ctxT(t)
	_ = s.Migrate(ctx)
	_ = s.Rollback(ctx)
	if v := version(t, s, e); v != "20" {
		t.Fatalf("version = %s", v)
	}
	e.out.Reset()
	_ = s.Status(ctx)
	lines := strings.Split(strings.TrimSpace(e.out.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("Status:\n%s", e.out.String())
	}
	for i, l := range lines[1:] {
		applied := i < 2
		if applied != strings.Contains(l, "applied") || applied == strings.Contains(l, " - ") {
			t.Fatalf("line %q: applied=%v", l, applied)
		}
	}
	if !strings.Contains(lines[3], "pending") || !strings.Contains(lines[3], "30_ledger.sql") {
		t.Fatalf("rolled-back migration not pending: %q", lines[3])
	}
}

// Create writes a scaffold that applies as is, and a hostile name cannot
// escape the folder. The folder must exist: the service is given it opened.
func TestSQLService_Create(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t)
	ctx := ctxT(t)
	if err := s.Create(ctx, "../../escape attempt"); err != nil {
		t.Fatal(err)
	}
	files := e.files("*")
	if len(files) != 1 || filepath.Dir(files[0]) != e.path || !strings.HasSuffix(files[0], "_escape_attempt.sql") {
		t.Fatalf("files = %v", files)
	}
	body := readFile(t, files[0])
	if !strings.Contains(body, "-- +goose Up") || !strings.Contains(body, "-- +goose Down") {
		t.Fatalf("scaffold:\n%s", body)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("scaffold does not apply: %v", err)
	}
	if err := s.Redo(ctx); err != nil {
		t.Fatalf("scaffold does not roll back: %v", err)
	}
	_ = os.RemoveAll(e.path)
	if err := s.Create(ctx, "x"); err == nil {
		t.Fatal("Create into a missing folder succeeded")
	}
}

func TestSQLService_FailedMigrationsAreAtomic(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "two_statements", "CREATE TABLE first (id int);\nCREATE TABLE first (id int);", "DROP TABLE first;")
	s := e.started(t)
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err == nil {
		t.Fatal("failing migration applied")
	}
	if hasTable(t, s, "first") {
		t.Fatal("first statement kept")
	}
	e.write(t, 1, "two_statements", "CREATE TABLE first (id int);\nCREATE TABLE second (id int);", "DROP TABLE second;\nDROP TABLE first;")
	if err := s.Migrate(ctx); err != nil || !hasTable(t, s, "second") {
		t.Fatalf("fixed migration: %v", err)
	}
}

func TestSQLService_RefusesOutOfOrderMigrations(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, 3)
	s := e.started(t)
	ctx := ctxT(t)
	_ = s.Migrate(ctx)
	e.write(t, 15, "merged_late", "CREATE TABLE late (id int);", "DROP TABLE late;")
	if err := s.Migrate(ctx); err == nil || hasTable(t, s, "late") {
		t.Fatalf("out-of-order migration: %v", err)
	}
}

func TestSQLService_ConcurrentStartupIsSafe(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	writeReleases(t, e, 1, len(bankReleases))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			s := ecsql.NewSQLService(e.dsn, 1, 2, e.dir, true, e.out, nil, nil)
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
	s := e.started(t)
	if n := count(t, s, "SELECT count(*) FROM goose_db_version WHERE version_id > 0"); n != len(bankReleases) {
		t.Fatalf("applied rows = %d, want %d (each exactly once)", n, len(bankReleases))
	}
}
