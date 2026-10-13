//go:build integration

package broker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
)

// A zero struct, a stopped service and a nil receiver.
func TestZeroValue_Absence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	zero := &broker.BrokerService{}
	if err := zero.Run(ctx); !errors.Is(err, broker.ErrNoTracker) {
		t.Errorf("zero Run = %v", err)
	}
	if err := zero.Publish(ctx, "t", nil, nil); !errors.Is(err, broker.ErrNotRunning) {
		t.Errorf("zero Publish = %v", err)
	}
	if err := zero.Stop(ctx); err != nil {
		t.Errorf("zero Stop = %v", err)
	}
	topic, group := names()
	b := service(t, group)
	_ = b.Stop(ctx)
	_ = b.Stop(ctx) // twice is fine
	if err := b.Publish(ctxT(t), topic, nil, []byte("x")); !errors.Is(err, broker.ErrNotRunning) {
		t.Errorf("publish after Stop = %v", err)
	}
	var ptr *broker.BrokerService
	mustPanic(t, "nil receiver", func() { _ = ptr.Run(ctx) })
}

// Stop while a handler is mid-retry neither hangs nor panics, and a second
// Stop is a no-op.
func TestZeroValue_StopDuringRetries(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	entered := make(chan struct{}, 1)
	_ = b.Subscribe(ctxT(t), topic, func(context.Context, []byte, []byte) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		return errors.New("always fails")
	})
	_ = b.Publish(ctxT(t), topic, []byte("k"), []byte("v"))
	<-entered
	for range 2 {
		if err := b.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
