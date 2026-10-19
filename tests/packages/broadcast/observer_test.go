//go:build integration

package broadcast_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func newObserver(t testing.TB) *observability.Service {
	t.Helper()
	return obstest.New(t)
}

func broadcastCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "broadcast", op, outcome)
}

func observed(t testing.TB, o *observability.Service) broadcast.IBroadcastService {
	t.Helper()
	requireServer(t)
	b := broadcast.NewBroadcastService(appID, appKey, appSecret, host, port, false, maxPayloadKB, nil, broadcast.WithObserver(o))
	if err := b.Start(ctxT(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	return b
}

// Start, ping and each dispatch are one counted operation.
func TestObserver_StartPingDispatchAreOperations(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	b := observed(t, o)
	ctx := ctxT(t)
	if err := b.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := b.Publish(ctx, channel(), "evt", map[string]any{"a": 1}); err != nil {
			t.Fatal(err)
		}
	}
	if got := broadcastCount(o, "start", "success"); got != 1 {
		t.Fatalf("start success = %v, want 1", got)
	}
	if got := broadcastCount(o, "ping", "success"); got != 1 {
		t.Fatalf("ping success = %v, want 1", got)
	}
	if got := broadcastCount(o, "dispatch", "success"); got != 3 {
		t.Fatalf("dispatch success = %v, want 3", got)
	}
}

// A call refused before any work (no channels, a bad name, an oversize payload) is not counted.
func TestObserver_RefusedDispatchIsNotCounted(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	b := observed(t, o)
	ctx := ctxT(t)
	if err := b.Dispatch(ctx, nil, "evt", "x"); !errors.Is(err, broadcast.ErrNoChannels) {
		t.Fatalf("no channels = %v", err)
	}
	if err := b.Dispatch(ctx, []string{"bad channel!"}, "evt", "x"); !errors.Is(err, broadcast.ErrInvalidChannel) {
		t.Fatalf("bad channel = %v", err)
	}
	if err := b.Dispatch(ctx, []string{channel()}, "pusher:evt", "x"); !errors.Is(err, broadcast.ErrInvalidEvent) {
		t.Fatalf("bad event = %v", err)
	}
	if err := b.Dispatch(ctx, []string{channel()}, "evt", strings.Repeat("x", (maxPayloadKB+1)<<10)); !errors.Is(err, broadcast.ErrPayloadTooLarge) {
		t.Fatalf("big payload = %v", err)
	}
	for _, outcome := range []string{"success", "error", "skipped"} {
		if got := broadcastCount(o, "dispatch", outcome); got != 0 {
			t.Fatalf("a refused dispatch was counted as %s: %v", outcome, got)
		}
	}
}

// A service that is not started counts nothing and returns ErrNotStarted.
func TestObserver_NotStartedIsNotCounted(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	b := broadcast.NewBroadcastService(appID, appKey, appSecret, "127.0.0.1", "1", false, 10, nil, broadcast.WithObserver(o))
	ctx := context.Background()
	if err := b.Publish(ctx, "c", "evt", "x"); !errors.Is(err, broadcast.ErrNotStarted) {
		t.Fatalf("Publish = %v", err)
	}
	if err := b.Ping(ctx); !errors.Is(err, broadcast.ErrNotStarted) {
		t.Fatalf("Ping = %v", err)
	}
	for _, op := range []string{"dispatch", "ping"} {
		for _, outcome := range []string{"success", "error"} {
			if got := broadcastCount(o, op, outcome); got != 0 {
				t.Fatalf("a call to a stopped service was counted: %s %s = %v", op, outcome, got)
			}
		}
	}
}

// An unreachable server is a counted error at start.
func TestObserver_UnreachableStartIsAnError(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	b := broadcast.NewBroadcastService(appID, appKey, appSecret, "127.0.0.1", "1", false, 10, nil, broadcast.WithObserver(o))
	if err := b.Start(context.Background()); err == nil {
		t.Fatal("Start against a closed port succeeded")
	}
	if got := broadcastCount(o, "start", "error"); got != 1 {
		t.Fatalf("start error = %v, want 1", got)
	}
}

// Neither a channel, an event nor a payload reaches the registry.
func TestObserver_NoChannelEventOrPayloadIsRecorded(t *testing.T) {
	t.Parallel()
	o := newObserver(t)
	b := observed(t, o)
	ch := channel("private-account-")
	if err := b.Publish(ctxT(t), ch, "balance-updated", map[string]any{"balance": "9999.99"}); err != nil {
		t.Fatal(err)
	}
	for _, sm := range o.Snapshot() {
		blob := sm.Name
		for k, v := range sm.Attrs {
			blob += " " + k + "=" + v
		}
		for _, leak := range []string{ch, "private-account", "balance-updated", "9999.99"} {
			if strings.Contains(blob, leak) {
				t.Fatalf("%q leaked into the registry: %s", leak, blob)
			}
		}
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	t.Parallel()
	b := started(t)
	if err := b.Publish(ctxT(t), channel(), "evt", "x"); err != nil {
		t.Fatal(err)
	}
	var zero broadcast.BroadcasterService
	if err := zero.Ping(context.Background()); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
