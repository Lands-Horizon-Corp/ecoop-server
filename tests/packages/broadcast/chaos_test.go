//go:build integration

package broadcast_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
)

// Chaos: the server disappears, slows down or drops connections.

func viaProxy(t *testing.T) (broadcast.IBroadcastService, *proxy) {
	t.Helper()
	p := newProxy(t)
	b := service(p.HostPort())
	if err := b.Start(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	return b, p
}

// With the server gone, publishing fails promptly; when it is back,
// publishing works again.
func TestChaos_ServerGoneAndBack(t *testing.T) {
	t.Parallel()
	b, p := viaProxy(t)
	ch := channel()
	p.Down()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	if err := b.Publish(ctx, ch, "e", 1); err == nil {
		t.Fatal("published with the server gone")
	}
	if d := time.Since(start); d > 4*time.Second {
		t.Fatalf("publish hung for %v", d)
	}
	if err := b.Ping(ctx); err == nil {
		t.Fatal("Ping healthy with the server gone")
	}
	p.Up()
	sub := connect(t)
	sub.subscribe(t, ch)
	var err error
	for range 3 { // a pooled connection may be dead once
		if err = b.Publish(ctxT(t), ch, "back", 1); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("publish after the server came back: %v", err)
	}
	sub.expect(t, 1, 10*time.Second)
}

// A slow server does not hold the caller past its context.
func TestChaos_SlowServerRespectsTheContext(t *testing.T) {
	t.Parallel()
	b, p := viaProxy(t)
	p.SetDelay(3 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := b.Publish(ctx, channel(), "e", 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Publish took %v past its deadline", d)
	}
}

// The heartbeat survives an outage: it skips the beats it cannot send and
// carries on.
func TestChaos_RunnerSurvivesAnOutage(t *testing.T) {
	t.Parallel()
	b, p := viaProxy(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Runner(ctx) }()
	p.Down()
	time.Sleep(2500 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Runner gave up during the outage: %v", err)
	default:
	}
	p.Up()
	sub := connect(t)
	sub.subscribe(t, broadcast.HeartbeatChannel)
	sub.expect(t, 1, 10*time.Second) // ours, or another test's: the channel is shared
}
