//go:build integration

package broker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
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
