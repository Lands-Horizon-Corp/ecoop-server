//go:build integration

package broadcast_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
)

// Zero value & absence: zero structs, nil and empty arguments, handles kept
// after Stop, nil contexts and nil receivers.

func TestZeroValue_ZeroStruct(t *testing.T) {
	t.Parallel()
	b := &broadcast.BroadcasterService{}
	ctx := context.Background()
	mustNotPanic(t, "zero struct", func() {
		if err := b.Start(ctx); !errors.Is(err, broadcast.ErrNoTracker) {
			t.Errorf("Start = %v", err)
		}
		for name, err := range map[string]error{
			"Ping": b.Ping(ctx), "Publish": b.Publish(ctx, "c", "e", 1), "Runner": b.Runner(ctx),
		} {
			if !errors.Is(err, broadcast.ErrNotStarted) {
				t.Errorf("%s = %v, want ErrNotStarted", name, err)
			}
		}
		if err := b.Stop(ctx); err != nil {
			t.Errorf("Stop = %v", err)
		}
	})
}

// Nil and empty payloads are valid JSON; nil and empty name lists are not
// destinations.
func TestZeroValue_NilAndEmpty(t *testing.T) {
	t.Parallel()
	b := started(t)
	ch := channel()
	sub := connect(t)
	sub.subscribe(t, ch)
	ctx := ctxT(t)
	var nilMap map[string]any
	var nilSlice []int
	for _, p := range []any{nil, nilMap, nilSlice, map[string]any{}, "", []byte{}} {
		if err := b.Publish(ctx, ch, "e", p); err != nil {
			t.Fatalf("payload %#v: %v", p, err)
		}
	}
	sub.expect(t, 6, 10*time.Second)
	if err := b.Dispatch(ctx, []string{}, "e", 1); !errors.Is(err, broadcast.ErrNoChannels) {
		t.Errorf("empty channels = %v", err)
	}
	if err := b.Broadcast(ctx, []string{ch}, []string{}, 1); !errors.Is(err, broadcast.ErrNoEvents) {
		t.Errorf("empty events = %v", err)
	}
}

// A handle kept after Stop refuses work instead of using a dead client.
func TestZeroValue_HandleAfterStop(t *testing.T) {
	t.Parallel()
	b := started(t)
	kept := b
	_ = b.Stop(context.Background())
	if err := kept.Publish(ctxT(t), channel(), "e", 1); !errors.Is(err, broadcast.ErrNotStarted) {
		t.Fatalf("publish through a handle kept after Stop = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := kept.Runner(ctx); !errors.Is(err, broadcast.ErrNotStarted) {
		t.Fatalf("Runner after Stop = %v", err)
	}
}

func TestZeroValue_NilContextFailsFast(t *testing.T) {
	t.Parallel()
	b := started(t)
	var nilCtx context.Context
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = capturePanic(func() { _ = b.Publish(nilCtx, channel(), "e", 1) })
		_, _ = capturePanic(func() { _ = b.Ping(nilCtx) })
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("a nil context hung")
	}
}

// A nil *BroadcasterService or nil interface is a wiring bug and fails
// loudly.
func TestZeroValue_NilReceiver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var ptr *broadcast.BroadcasterService
	var iface broadcast.BroadcasterServices
	ops := map[string]func(broadcast.BroadcasterServices){
		"Start":   func(b broadcast.BroadcasterServices) { _ = b.Start(ctx) },
		"Publish": func(b broadcast.BroadcasterServices) { _ = b.Publish(ctx, "c", "e", 1) },
	}
	for name, op := range ops {
		t.Run(name+"/nil pointer", func(t *testing.T) { mustPanic(t, name, func() { op(ptr) }) })
		t.Run(name+"/nil interface", func(t *testing.T) { mustPanic(t, name, func() { op(iface) }) })
	}
}
