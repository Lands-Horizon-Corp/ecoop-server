package sql_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// Zero value & absence: zero structs, nil and empty dependencies, closed or
// vanished files, handles kept after Stop, nil contexts and nil receivers.

func TestZeroValue_ZeroStruct(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := &ecsql.SQLService{}
	mustNotPanic(t, "zero struct", func() {
		if err := s.Start(ctx); !errors.Is(err, ecsql.ErrNoDSN) {
			t.Errorf("Start = %v", err)
		}
		if s.Client() != nil {
			t.Error("Client not nil")
		}
		for name, err := range map[string]error{
			"Ping": s.Ping(ctx), "Migrate": s.Migrate(ctx), "Fresh": s.Fresh(ctx),
			"Rollback": s.Rollback(ctx), "RollbackTo": s.RollbackTo(ctx, 1), "Redo": s.Redo(ctx),
			"RollbackSteps": s.RollbackSteps(ctx, 1), "UpSteps": s.UpSteps(ctx, 1),
			"Status": s.Status(ctx), "Version": s.Version(ctx),
		} {
			if !errors.Is(err, ecsql.ErrNotStarted) {
				t.Errorf("%s = %v, want ErrNotStarted", name, err)
			}
		}
		if err := s.Run(ctx); err != nil {
			t.Errorf("Run with autoMigrate off = %v", err)
		}
		if err := s.Create(ctx, "x"); !errors.Is(err, ecsql.ErrNoMigrationsDir) {
			t.Errorf("Create = %v", err)
		}
		if err := s.Stop(ctx); err != nil {
			t.Errorf("Stop = %v", err)
		}
	})
}

func TestZeroValue_StopBeforeStartAndTwice(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.service()
	for range 2 {
		if err := s.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Start(ctxT(t))
	for range 3 {
		if err := s.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestZeroValue_NilAndEmptyModels(t *testing.T) {
	t.Parallel()
	for name, models := range map[string][]any{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.write(t, 1, "a", "CREATE TABLE a (id int);", "DROP TABLE a;")
			s := e.started(t, models...)
			if err := s.Migrate(ctxT(t)); err != nil {
				t.Fatal(err)
			}
			if len(e.files("*_auto.sql")) != 0 || !hasTable(t, s, "a") {
				t.Fatal("Migrate without models must only apply the files")
			}
		})
	}
}

func TestZeroValue_NilOutputUsesStdout(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := ecsql.NewSQLService(e.dsn, 1, 2, e.dir, false, nil, nil, nil)
	if err := s.Start(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	mustNotPanic(t, "Version with nil output", func() {
		if err := s.Version(ctxT(t)); err != nil {
			t.Fatal(err)
		}
	})
}

func TestZeroValue_PoolSizeZeroMeansDefaults(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := ecsql.NewSQLService(e.dsn, 0, 0, e.dir, false, e.out, nil, nil)
	if err := s.Start(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	if err := s.Ping(ctxT(t)); err != nil {
		t.Fatal(err)
	}
}

// Dangling pointers: the migrations *os.File closed by its owner, the folder
// deleted under the service, and a Client handle kept after Stop.

func TestZeroValue_ClosedMigrationsFileStillWorks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "a", "CREATE TABLE a (id int);", "DROP TABLE a;")
	_ = e.dir.Close()
	s := e.started(t)
	if err := s.Migrate(ctxT(t)); err != nil {
		t.Fatalf("Migrate after the *os.File was closed: %v", err)
	}
	if err := s.Create(ctxT(t), "next"); err != nil {
		t.Fatalf("Create after the *os.File was closed: %v", err)
	}
}

func TestZeroValue_MigrationsFolderDeleted(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t)
	if err := os.RemoveAll(e.path); err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t)
	for name, fn := range map[string]func() error{
		"Migrate": func() error { return s.Migrate(ctx) },
		"Status":  func() error { return s.Status(ctx) },
		"Create":  func() error { return s.Create(ctx, "x") },
	} {
		var err error
		mustNotPanic(t, name, func() { err = fn() })
		if err == nil {
			t.Errorf("%s succeeded with the folder gone", name)
		}
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("the connection must not depend on the folder: %v", err)
	}
}

func TestZeroValue_ClientHandleAfterStop(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.service()
	_ = s.Start(ctxT(t))
	db := s.Client()
	_ = s.Stop(context.Background())
	mustNotPanic(t, "query on a stopped handle", func() {
		if _, err := db.ExecContext(ctxT(t), "SELECT 1"); err == nil {
			t.Fatal("query on a closed pool succeeded")
		}
	})
}

func TestZeroValue_NilContext(t *testing.T) {
	t.Parallel()
	// A nil ctx is a programming error: it must fail fast, not hang.
	e := newEnv(t)
	s := e.started(t)
	var nilCtx context.Context
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, fn := range []func(){
			func() { _ = s.Ping(nilCtx) },
			func() { _ = s.Migrate(nilCtx) },
			func() { _ = s.Status(nilCtx) },
			func() { _ = s.Stop(nilCtx) },
		} {
			_, _ = capturePanic(fn)
		}
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("nil ctx hung")
	}
}

// A nil *SQLService or nil interface is a wiring bug and must fail loudly.
func TestZeroValue_NilReceiver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var ptr *ecsql.SQLService
	var iface ecsql.SQLServices
	ops := map[string]func(ecsql.SQLServices){
		"Start":   func(s ecsql.SQLServices) { _ = s.Start(ctx) },
		"Ping":    func(s ecsql.SQLServices) { _ = s.Ping(ctx) },
		"Migrate": func(s ecsql.SQLServices) { _ = s.Migrate(ctx) },
		"Create":  func(s ecsql.SQLServices) { _ = s.Create(ctx, "x") },
		"Client":  func(s ecsql.SQLServices) { _ = s.Client() },
	}
	for name, op := range ops {
		t.Run(name+"/nil pointer", func(t *testing.T) { mustPanic(t, name, func() { op(ptr) }) })
		t.Run(name+"/nil interface", func(t *testing.T) { mustPanic(t, name, func() { op(iface) }) })
	}
}
