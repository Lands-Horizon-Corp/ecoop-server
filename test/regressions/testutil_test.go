package regressions

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Shared primitives for every regression test in this package.
//
// File layout: service.<area>_test.go holds the tests for one service; fixtures and helpers that
// several tests share live beside them (service.sql_env_test.go, service.sql_bank_test.go, ...).
// Tests are named Test<Area>_<Scenario>; the migration suites add Happy/Sad after the phase name.
// Tests that need Docker call requireReachable, so they fail (never skip) when `make test-up` was not run.

// bg is the context used by tests that do not need cancellation.
var bg = context.Background()

// TestMain keeps third-party logging (goose, go-redis) out of the output unless -v is set.
func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(io.Discard)
	}
	os.Exit(m.Run())
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// requireReachable fails the test when nothing listens on addr, pointing at the compose services.
func requireReachable(t testing.TB, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("%s is not reachable (%v); run: make test-up", addr, err)
	}
	_ = conn.Close()
}

// closedAddr returns a local address nothing is listening on.
func closedAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// walkFiles calls fn for every regular file under root.
func walkFiles(root string, fn func(path string)) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			fn(path)
		}
		return nil
	})
}

// second drops the first return value so a (value, error) call fits a single-error table.
func second[T any](_ T, err error) error {
	return err
}

// runParallel runs fn n times concurrently under a deadline and returns every error, so a deadlock
// shows up as a context timeout instead of a hung test.
func runParallel(t *testing.T, n int, fn func(ctx context.Context, i int) error) []error {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 60*time.Second)
	defer cancel()
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for i := range n {
		wg.Go(func() {
			if err := fn(ctx, i); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return errs
}

// noPanic runs fn and fails the test, naming the call, if it panics.
func noPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", name, r)
		}
	}()
	fn()
}
