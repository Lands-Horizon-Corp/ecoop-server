//go:build unix

// Package suitelock makes the heavy integration suites take turns.
//
// The cqrs, sql and pagination suites each create and drop databases and open
// many connections on the one shared Postgres. Run at the same time they starve
// each other: i/o timeouts and migration-lock deadlines that have nothing to do
// with the code under test. `go test ./...` runs packages in parallel, so the
// suites cannot rely on being started one by one. Each takes this lock in
// TestMain; a second suite waits for the first to finish.
//
// The lock is an advisory flock on a file in the temp directory. The kernel
// releases it when the process ends, however it ends (a panic, a timeout, a
// kill), so a crashed suite can never leave the others stuck.
package suitelock

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// lockPath is the lock file. One per machine and user: every suite that shares a
// Postgres shares it.
var lockPath = filepath.Join(os.TempDir(), "ecoop-heavy-suites.lock")

// Acquire blocks until this process holds the lock, and returns the function that
// releases it (calling it twice is harmless). It waits at most max; after that it
// returns an error instead of hanging a CI job forever behind a stuck suite.
func Acquire(who string, max time.Duration) (release func(), err error) {
	path := lockPath
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("suitelock: opening %s: %w", path, err)
	}
	deadline := time.Now().Add(max)
	announced := false
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EINTR {
			_ = f.Close()
			return nil, fmt.Errorf("suitelock: locking %s: %w", path, err)
		}
		if time.Now().After(deadline) {
			_ = f.Close()
			return nil, fmt.Errorf("suitelock: another heavy suite held %s for more than %s", path, max)
		}
		if !announced {
			fmt.Fprintf(os.Stderr, "%s: waiting for the other heavy test suite to finish (%s)\n", who, path)
			announced = true
		}
		time.Sleep(250 * time.Millisecond)
	}
	// Say who holds it, for whoever is waiting. Best effort: the lock is the flock.
	_ = f.Truncate(0)
	_, _ = fmt.Fprintf(f, "%s pid=%d since=%s\n", who, os.Getpid(), time.Now().UTC().Format(time.RFC3339))
	released := false
	return func() {
		if released {
			return
		}
		released = true
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
