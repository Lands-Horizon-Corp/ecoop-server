package sql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// Chaos: the network and the server misbehave under a running service:
// dropped connections, killed sessions, blocked locks, slow migrations.

// proxied starts a service on e that reaches Postgres through a proxy.
func proxied(t *testing.T, e *env, p *proxy) ecsql.SQLServices {
	t.Helper()
	s := ecsql.NewSQLService(p.DSN(e.dsn), 2, 4, e.dir, false, e.out, nil, nil)
	if err := s.Start(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s
}

func TestChaos_PoolRecoversAfterConnectionsDrop(t *testing.T) {
	t.Parallel()
	e, p := newEnv(t), newProxy(t)
	s := proxied(t, e, p)
	ctx := ctxT(t)
	for i := range 5 {
		p.DropAll()
		var err error
		for range 3 { // a dead pooled conn may fail once before it is replaced
			if err = s.Ping(ctx); err == nil {
				break
			}
		}
		if err != nil {
			t.Fatalf("round %d: pool did not recover: %v", i, err)
		}
	}
}

// Cutting the connection mid-migration fails it, rolls it back, releases
// the session advisory lock, and a retry applies it exactly once.
func TestChaos_ConnectionCutMidMigration(t *testing.T) {
	t.Parallel()
	e, p := newEnv(t), newProxy(t)
	e.write(t, 1, "slow", "CREATE TABLE slow (id int);\nSELECT pg_sleep(2);", "DROP TABLE slow;")
	s := proxied(t, e, p)
	ctx := ctxT(t)
	errc := make(chan error, 1)
	go func() { errc <- s.Migrate(ctx) }()
	time.Sleep(500 * time.Millisecond)
	p.DropAll()
	if err := <-errc; err == nil {
		t.Fatal("Migrate survived its connection being cut")
	}
	direct := e.started(t)
	if hasTable(t, direct, "slow") {
		t.Fatal("cut migration left its table behind")
	}
	retry, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := direct.Migrate(retry); err != nil {
		t.Fatalf("retry (lock stuck?): %v", err)
	}
	if v := version(t, direct, e); v != "1" {
		t.Fatalf("version = %s", v)
	}
}

// The server killing the migrating session is the same as a cut connection.
func TestChaos_BackendTerminatedMidMigration(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "slow", "CREATE TABLE slow (id int);\nSELECT pg_sleep(3);", "DROP TABLE slow;")
	s := e.started(t)
	ctx := ctxT(t)
	errc := make(chan error, 1)
	go func() { errc <- s.Migrate(ctx) }()
	db := dbName(e.dsn)
	deadline := time.Now().Add(5 * time.Second)
	for {
		var n int
		_ = admin.QueryRowContext(ctx, "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = $1 AND query LIKE '%pg_sleep%' AND pid <> pg_backend_pid()", db).Scan(&n)
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := <-errc; err == nil {
		t.Fatal("Migrate survived its backend being terminated")
	}
	e.write(t, 1, "slow", "CREATE TABLE slow (id int);", "DROP TABLE slow;")
	retry, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var err error
	for range 3 { // the pool may hand out the killed connection once
		if err = s.Migrate(retry); err == nil {
			break
		}
	}
	if err != nil || !hasTable(t, s, "slow") {
		t.Fatalf("retry: %v", err)
	}
}

// A long transaction holding a lock the migration needs makes it time out
// with no change; once the blocker is gone it applies.
func TestChaos_LockContentionTimesOutWithoutChange(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "base", "CREATE TABLE acct (id int);", "DROP TABLE acct;")
	s := e.started(t)
	ctx := ctxT(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tx, err := s.Client().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "LOCK TABLE acct IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	e.write(t, 2, "alter", "ALTER TABLE acct ADD COLUMN bal int;", "ALTER TABLE acct DROP COLUMN bal;")
	short, cancel := context.WithTimeout(ctx, time.Second)
	start := time.Now()
	err = s.Migrate(short)
	cancel()
	if err == nil {
		t.Fatal("Migrate ran through a held lock")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("Migrate ignored its deadline: %v", d)
	}
	_ = tx.Rollback()
	if v := version(t, s, e); v != "1" {
		t.Fatalf("version = %s after timed-out migration", v)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate after blocker left: %v", err)
	}
	if v := version(t, s, e); v != "2" {
		t.Fatalf("version = %s", v)
	}
}

// Postgres going away entirely is reported, not reported as healthy.
func TestChaos_DatabaseGoneIsDetected(t *testing.T) {
	t.Parallel()
	e, p := newEnv(t), newProxy(t)
	s := proxied(t, e, p)
	_ = p.ln.Close() // no new connections
	p.DropAll()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Ping(ctx); err == nil {
		t.Fatal("Ping healthy with the database gone")
	}
	if err := s.Status(ctx); err == nil || errors.Is(err, ecsql.ErrNotStarted) {
		t.Fatalf("Status = %v, want a connection error", err)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop with the database gone: %v", err)
	}
}

func TestChaos_TrafficWhileMigrating(t *testing.T) {
	t.Parallel()
	e, p := newEnv(t), newProxy(t)
	for v := int64(1); v <= 5; v++ {
		e.write(t, v, "m", "SELECT 1;", "SELECT 1;")
	}
	s := proxied(t, e, p)
	ctx := ctxT(t)
	errc := make(chan error, 1)
	go func() { errc <- s.Migrate(ctx) }()
	for range 3 { // churn while migrating
		time.Sleep(20 * time.Millisecond)
		_ = s.Ping(ctx)
	}
	if err := <-errc; err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if v := version(t, s, e); v != "5" {
		t.Fatalf("version = %s", v)
	}
}
