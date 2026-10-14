//go:build !unix

// Package suitelock makes the heavy integration suites take turns. On platforms
// without flock it does nothing: run the suites one at a time with -p 1.
package suitelock

import "time"

// Acquire is a no-op here.
func Acquire(string, time.Duration) (release func(), err error) { return func() {}, nil }
