package inflight_test

import (
	"context"
	"testing"
	"time"
)

func capturePanic(fn func()) (recovered any) {
	defer func() { recovered = recover() }()
	fn()
	return nil
}

func mustNotPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	if r := capturePanic(fn); r != nil {
		t.Fatalf("%s panicked: %v", name, r)
	}
}

func mustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	if r := capturePanic(fn); r == nil {
		t.Fatalf("%s: expected panic, got none", name)
	}
}

// waitAsync runs Wait in the background and returns its result channel.
func waitAsync(f interface{ Wait(context.Context) error }, ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- f.Wait(ctx) }()
	return ch
}

// blocked reports whether ch has not delivered within d.
func blocked(ch <-chan error, d time.Duration) bool {
	select {
	case <-ch:
		return false
	case <-time.After(d):
		return true
	}
}

func recv(t *testing.T, ch <-chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(within):
		t.Fatalf("Wait did not return within %v", within)
		return nil
	}
}
