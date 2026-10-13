//go:build integration

package broker_test

import (
	"context"
	"errors"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
	"sync/atomic"
	"testing"
	"time"
)

// A handler that fails transiently (a downstream timeout) is retried until
// it succeeds.
func TestChaos_TransientHandlerFailures(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	var calls atomic.Int64
	var in inbox
	_ = b.Subscribe(ctxT(t), topic, func(ctx context.Context, k, v []byte) error {
		if calls.Add(1) <= 2 {
			return errors.New("ledger service timed out")
		}
		return in.handle(ctx, k, v)
	})
	_ = b.Publish(ctxT(t), topic, []byte("k"), []byte("v"))
	if got := in.wait(t, 1); got[0] != "k=v" {
		t.Fatalf("received %v", got)
	}
}

// A handler that recovers in time is never dead-lettered.
func TestChaos_RecoveredMessageIsNotParked(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b := service(t, group)
	var calls atomic.Int64
	var in inbox
	_ = b.Subscribe(ctxT(t), topic, func(ctx context.Context, k, v []byte) error {
		if calls.Add(1) < broker.HandlerAttempts {
			return errors.New("ledger service timed out")
		}
		return in.handle(ctx, k, v)
	})
	_ = b.Publish(ctxT(t), topic, []byte("k"), []byte("v"))
	in.wait(t, 1)
	if n := countFor(t, topic+broker.DLQSuffix, 2*time.Second); n != 0 {
		t.Fatalf("%d records on the DLQ, want none", n)
	}
}
