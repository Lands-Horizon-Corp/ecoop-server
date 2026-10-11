package sql_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// Sad path: bad configuration and calls in the wrong state are refused with
// the right error and change nothing.

func TestSad_StartValidation(t *testing.T) {
	t.Parallel()
	dir, _ := migrationsDir(t)
	ctx := context.Background()
	cases := []struct {
		name       string
		dsn        string
		idle, open int
		want       error
	}{
		{"empty dsn", "", 1, 1, ecsql.ErrNoDSN},
		{"negative open", "postgres://x@127.0.0.1:1/db", 1, -1, ecsql.ErrInvalidPool},
		{"negative idle", "postgres://x@127.0.0.1:1/db", -1, 1, ecsql.ErrInvalidPool},
	}
	for _, c := range cases {
		s := ecsql.NewSQLService(c.dsn, c.idle, c.open, dir, false, nil, nil, nil)
		if err := s.Start(ctx); !errors.Is(err, c.want) {
			t.Errorf("%s: Start = %v, want %v", c.name, err, c.want)
		}
		if s.Client() != nil {
			t.Errorf("%s: Client set after failed Start", c.name)
		}
	}
}

func TestSad_UnreachableAndWrongPassword(t *testing.T) {
	t.Parallel()
	requirePG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	unreachable := ecsql.NewSQLService("postgres://postgres:pw@127.0.0.1:1/postgres?sslmode=disable", 1, 1, nil, false, nil, nil, nil)
	if err := unreachable.Start(ctx); err == nil {
		t.Fatal("Start on a closed port succeeded")
	}
	const secret = "definitely-wrong-pw"
	wrong := ecsql.NewSQLService(strings.Replace(dsnFor("postgres"), pgPassword, secret, 1), 1, 1, nil, false, nil, nil, nil)
	err := wrong.Start(ctx)
	if err == nil {
		t.Fatal("Start with a wrong password succeeded")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaks the password: %v", err)
	}
	if unreachable.Client() != nil || wrong.Client() != nil {
		t.Fatal("Client set after failed Start")
	}
}

func TestSad_CommandsBeforeStartAndAfterStop(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := ecsql.NewSQLService(e.dsn, 1, 2, e.dir, true, e.out, nil, nil)
	ctx := ctxT(t)
	check := func(phase string) {
		t.Helper()
		for name, err := range map[string]error{
			"Ping": s.Ping(ctx), "Run": s.Run(ctx), "Migrate": s.Migrate(ctx), "Fresh": s.Fresh(ctx),
			"Rollback": s.Rollback(ctx), "RollbackTo": s.RollbackTo(ctx, 0), "Redo": s.Redo(ctx),
			"RollbackSteps": s.RollbackSteps(ctx, 1), "UpSteps": s.UpSteps(ctx, 1),
			"Status": s.Status(ctx), "Version": s.Version(ctx),
		} {
			if !errors.Is(err, ecsql.ErrNotStarted) {
				t.Errorf("%s: %s = %v, want ErrNotStarted", phase, name, err)
			}
		}
	}
	check("before Start")
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	check("after Stop")
	if s.Client() != nil {
		t.Fatal("Client still set after Stop")
	}
}

func TestSad_InvalidSteps(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "a", "CREATE TABLE a (id int);", "DROP TABLE a;")
	s := e.started(t)
	ctx := ctxT(t)
	for _, n := range []int{0, -1, -100} {
		if err := s.UpSteps(ctx, n); !errors.Is(err, ecsql.ErrInvalidSteps) {
			t.Errorf("UpSteps(%d) = %v", n, err)
		}
		if err := s.RollbackSteps(ctx, n); !errors.Is(err, ecsql.ErrInvalidSteps) {
			t.Errorf("RollbackSteps(%d) = %v", n, err)
		}
	}
	if v := version(t, s, e); v != "0" {
		t.Fatalf("refused steps changed the version to %s", v)
	}
}

func TestSad_MigrationsDirectoryProblems(t *testing.T) {
	t.Parallel()
	dsn := newDatabase(t)
	ctx := ctxT(t)

	none := ecsql.NewSQLService(dsn, 1, 2, nil, true, nil, nil, nil)
	if err := none.Create(ctx, "x"); !errors.Is(err, ecsql.ErrNoMigrationsDir) {
		t.Errorf("Create without dir = %v", err)
	}
	if err := none.Start(ctx); err != nil {
		t.Fatalf("Start without dir must still connect: %v", err)
	}
	defer none.Stop(ctx)
	if err := none.Migrate(ctx); !errors.Is(err, ecsql.ErrNoMigrationsDir) {
		t.Errorf("Migrate without dir = %v", err)
	}
	if err := none.Status(ctx); !errors.Is(err, ecsql.ErrNoMigrationsDir) {
		t.Errorf("Status without dir = %v", err)
	}

	file := filepath.Join(t.TempDir(), "not-a-dir.sql")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(file)
	defer f.Close()
	notDir := ecsql.NewSQLService(dsn, 1, 2, f, false, nil, nil, nil)
	if err := notDir.Create(ctx, "x"); !errors.Is(err, ecsql.ErrNotADirectory) {
		t.Errorf("Create into a file = %v", err)
	}
	if err := notDir.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer notDir.Stop(ctx)
	if err := notDir.Migrate(ctx); !errors.Is(err, ecsql.ErrNotADirectory) {
		t.Errorf("Migrate into a file = %v", err)
	}
}

func TestSad_CreateRejectsEmptyNames(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.service()
	for _, name := range []string{"", "   ", "!!!", "///", "日本語", "--__--"} {
		if err := s.Create(ctxT(t), name); !errors.Is(err, ecsql.ErrEmptyName) {
			t.Errorf("Create(%q) = %v, want ErrEmptyName", name, err)
		}
	}
	if got := e.files("*"); len(got) != 0 {
		t.Fatalf("refused names wrote files: %v", got)
	}
}

func TestSad_RollbackToUnknownOrFutureVersion(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "a", "CREATE TABLE a (id int);", "DROP TABLE a;")
	s := e.started(t)
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = s.RollbackTo(ctx, 99) // ahead of current: nothing to revert
	if v := version(t, s, e); v != "1" {
		t.Fatalf("RollbackTo(future) changed version to %s", v)
	}
	if err := s.RollbackTo(ctx, -1); err == nil {
		t.Error("RollbackTo(-1) accepted")
	}
	if v := version(t, s, e); v != "1" {
		t.Fatalf("RollbackTo(-1) changed version to %s", v)
	}
}

func TestSad_StartWithCancelledContext(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := e.service()
	if err := s.Start(ctx); err == nil {
		t.Fatal("Start with a cancelled ctx succeeded")
	}
	if s.Client() != nil {
		t.Fatal("Client set after failed Start")
	}
}
