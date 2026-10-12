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
