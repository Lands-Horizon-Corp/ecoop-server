//go:build integration

package sql_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Sync & race: one service used from many goroutines, and Stop racing the
// work it must wait for. Run with `go test -race`.

const workers = 16

// Stop waits for a running migration: it is never cut off halfway.
func TestRace_StopWaitsForRunningMigration(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "slow", "CREATE TABLE slow (id int);\nSELECT pg_sleep(1);", "DROP TABLE slow;")
	s := e.service()
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	var finished time.Time
	go func() { err := s.Migrate(ctx); finished = time.Now(); errc <- err }()
	time.Sleep(200 * time.Millisecond)
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	stopped := time.Now()
	if err := <-errc; err != nil {
		t.Fatalf("migration cut off by Stop: %v", err)
	}
	if finished.After(stopped) {
		t.Fatal("Stop returned before the migration finished")
	}
	check := e.started(t)
	if v := version(t, check, e); v != "1" {
		t.Fatalf("version = %s", v)
	}
}

// When ctx ends first Stop returns its error at once, the migration still
// completes, and the pool is closed afterwards.
func TestRace_StopGivesUpButMigrationCompletes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "slow", "CREATE TABLE slow (id int);\nSELECT pg_sleep(1.5);", "DROP TABLE slow;")
	s := e.service()
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- s.Migrate(ctx) }()
	time.Sleep(200 * time.Millisecond)
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := s.Stop(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Stop ignored its deadline: %v", d)
	}
	if err := s.Ping(ctx); !errors.Is(err, ecsql.ErrNotStarted) {
		t.Fatalf("Ping after Stop = %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("migration failed after Stop gave up: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.Client() != nil {
		if time.Now().After(deadline) {
			t.Fatal("pool never closed after the migration finished")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Restarting right after a Stop that gave up must not have the old Stop's
// background close tear down the new pool.
func TestRace_RestartAfterStopGaveUpKeepsNewPool(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "slow", "SELECT pg_sleep(1);", "SELECT 1;")
	s := e.service()
	ctx := ctxT(t)
	_ = s.Start(ctx)
	errc := make(chan error, 1)
	go func() { errc <- s.Migrate(ctx) }()
	time.Sleep(200 * time.Millisecond)
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	_ = s.Stop(short)
	cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(ctx)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // let the background close run
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("new pool closed by the old Stop: %v", err)
	}
	if _, err := s.Client().ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatalf("Client after restart: %v", err)
	}
}

func TestRace_ConcurrentStartLeavesOnePool(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.service()
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			if err := s.Start(ctxT(t)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	defer s.Stop(context.Background())
	db := s.Client()
	if err := s.Start(ctxT(t)); err != nil || s.Client() != db {
		t.Fatal("Start while started replaced the pool")
	}
	var conns int
	_ = admin.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE datname = $1", dbName(e.dsn)).Scan(&conns)
	if conns > 2 {
		t.Fatalf("%d connections open; concurrent Starts leaked pools", conns)
	}
}

func TestRace_LifecycleChurnWithTraffic(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "a", "CREATE TABLE a (id int);", "DROP TABLE a;")
	s := e.service()
	ctx := ctxT(t)
	_ = s.Start(ctx)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				mustNotPanic(t, "op during churn", func() {
					var err error
					switch i % 4 {
					case 0:
						err = s.Ping(ctx)
					case 1:
						err = s.Status(ctx)
					case 2:
						err = s.Migrate(ctx)
					case 3:
						err = s.Version(ctx)
					}
					if err != nil && !errors.Is(err, ecsql.ErrNotStarted) {
						t.Errorf("worker %d: %v", i, err)
					}
				})
				time.Sleep(time.Millisecond)
			}
		})
	}
	for i := range 20 {
		if i%2 == 0 {
			if err := s.Stop(ctx); err != nil {
				t.Error(err)
			}
		} else if err := s.Start(ctx); err != nil {
			t.Error(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	_ = s.Stop(ctx)
}

// Concurrent Creates in one process must each get their own version.
func TestRace_ConcurrentCreateGivesDistinctVersions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.service()
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			if err := s.Create(ctxT(t), "same name"); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	files := e.files("*.sql")
	if len(files) != workers {
		t.Fatalf("%d files for %d Creates: concurrent Creates overwrote each other", len(files), workers)
	}
	seen := map[string]bool{}
	for _, f := range files {
		v, _, _ := strings.Cut(filepath.Base(f), "_")
		if seen[v] {
			t.Fatalf("version %s used twice", v)
		}
		seen[v] = true
	}
}

// Concurrent Migrates on one service generate one file and apply it once.
func TestRace_ConcurrentMigrateOnOneService(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.started(t, (*MemberV1)(nil), (*Loan)(nil))
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			if err := s.Migrate(ctxT(t)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if got := e.files("*.sql*"); len(got) != 1 {
		t.Fatalf("files = %v", got)
	}
}

// A shared tracker: stopping one service waits for the other's work too.
func TestRace_SharedTrackerStopWaitsForSibling(t *testing.T) {
	t.Parallel()
	shared := utils.NewInflight()
	a, b := newEnv(t), newEnv(t)
	b.write(t, 1, "slow", "SELECT pg_sleep(0.5);", "SELECT 1;")
	ctx := ctxT(t)
	sa := ecsql.NewSQLService(a.dsn, 1, 2, a.dir, false, a.out, nil, shared)
	sb := ecsql.NewSQLService(b.dsn, 1, 2, b.dir, false, b.out, nil, shared)
	_ = sa.Start(ctx)
	_ = sb.Start(ctx)
	defer sb.Stop(ctx)
	errc := make(chan error, 1)
	go func() { errc <- sb.Migrate(ctx) }()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	if err := sa.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 250*time.Millisecond {
		t.Fatalf("a.Stop took %v; it should wait for b's migration on the shared tracker", d)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}
