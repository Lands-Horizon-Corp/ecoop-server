//go:build integration

package cqrs_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/uptrace/bun/driver/pgdriver"
)

// Fault-injection helpers shared by the chaos, failover, shutdown and pool tests.

// holdRead opens its own connection to the test's read database, runs query in
// a transaction that stays open, and returns the connection's backend pid and
// a release that rolls the transaction back. While it is open, whatever it
// locked blocks the service.
func holdRead(t testing.TB, p *pair, query string) (pid int, release func()) {
	t.Helper()
	db := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(p.readSrv.dsn(p.readDB)), pgdriver.WithReadTimeout(0)))
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow("SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(query); err != nil {
		t.Fatalf("hold %q: %v", query, err)
	}
	var once bool
	release = func() {
		if once {
			return
		}
		once = true
		_ = tx.Rollback()
		_ = db.Close()
	}
	t.Cleanup(release)
	return pid, release
}

// killReadBackends terminates every connection to the test's read database
// except pid, as a failover or a restarted server would.
func killReadBackends(t testing.TB, p *pair, except int) {
	t.Helper()
	if _, err := readAdmin.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname = $1 AND pid <> $2 AND pid <> pg_backend_pid()`, p.readDB, except); err != nil {
		t.Fatal(err)
	}
}

// setReadOnly makes new sessions on the read database read-only (true), as a
// replica is until it is promoted, or writable again (false).
func setReadOnly(t testing.TB, p *pair, on bool) {
	t.Helper()
	q := fmt.Sprintf(`ALTER DATABASE %q RESET default_transaction_read_only`, p.readDB)
	if on {
		q = fmt.Sprintf(`ALTER DATABASE %q SET default_transaction_read_only = on`, p.readDB)
	}
	if _, err := readAdmin.Exec(q); err != nil {
		t.Fatal(err)
	}
}

// job is a function running in the background. Looking at whether it has
// finished never consumes its result, so a test can check on it and wait for it.
type job struct {
	done chan struct{}
	err  error
}

func ctxBackground() context.Context { return context.Background() }

// killer terminates the connections to the read database again and again.
type killer struct {
	stopCh chan struct{}
	done   chan struct{}
}

func startKiller(r *rig) *killer {
	k := &killer{stopCh: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(k.done)
		for {
			select {
			case <-k.stopCh:
				return
			case <-time.After(20 * time.Millisecond):
				_, _ = readAdmin.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
					WHERE datname = $1 AND pid <> pg_backend_pid()`, r.p.readDB)
			}
		}
	}()
	r.t.Cleanup(k.stop)
	return k
}

func (k *killer) stop() {
	select {
	case <-k.stopCh:
	default:
		close(k.stopCh)
	}
	<-k.done
}

// settle returns once the read database answers queries again after faults were
// injected, so a test's own checking reads do not trip over the last one.
func settle(t testing.TB, p *pair) {
	t.Helper()
	setReadOnly(t, p, false)
	deadline := time.Now().Add(15 * time.Second)
	for {
		var one int
		err := p.read.Client().NewRaw("SELECT 1").Scan(context.Background(), &one)
		if err == nil {
			// Several in a row: every pooled connection has been replaced or is alive.
			ok := true
			for range 8 {
				if p.read.Client().NewRaw("SELECT 1").Scan(context.Background(), &one) != nil {
					ok = false
					break
				}
			}
			if ok {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the read database never recovered: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// async runs fn in the background.
func async(fn func() error) *job {
	j := &job{done: make(chan struct{})}
	go func() {
		defer close(j.done)
		j.err = fn()
	}()
	return j
}

// stays reports whether j is still running after d.
func stays(j *job, d time.Duration) bool {
	select {
	case <-j.done:
		return false
	case <-time.After(d):
		return true
	}
}

// within waits up to d for j and returns its result.
func within(t testing.TB, j *job, d time.Duration, what string) error {
	t.Helper()
	select {
	case <-j.done:
		return j.err
	case <-time.After(d):
		t.Fatalf("%s did not finish within %s", what, d)
		return nil
	}
}
