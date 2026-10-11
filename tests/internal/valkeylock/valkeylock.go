// Package valkeylock serialises test packages that run queue workers on the
// dev Valkey. go test runs packages in parallel, and two packages' workers on
// the same queues would pick up each other's jobs.
package valkeylock

import (
	"os"
	"path/filepath"
	"syscall"
)

// Acquire blocks until no other such test package holds the lock, and
// returns the func that releases it.
func Acquire() (release func(), err error) {
	f, err := os.OpenFile(filepath.Join(os.TempDir(), "ecoop-valkey-tests.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
