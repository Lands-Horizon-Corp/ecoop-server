//go:build integration

package cdc_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// A downstream that fails a couple of times is retried until the change is
// applied.
func TestChaos_TransientHandlerFailures(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	var calls atomic.Int64
	var in inbox
	_ = s.Subscribe(ctxT(t), topic, func(ctx context.Context, k json.RawMessage, ev cdc.ChangeEvent) error {
		if calls.Add(1) <= 2 {
			return errors.New("search index timed out")
		}
		return in.handle(ctx, k, ev)
	})
	_ = producer(t).Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "x"))
	in.wait(t, 1)
}

// A change whose handler recovers in time is never dead-lettered.
func TestChaos_RecoveredChangeIsNotParked(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	var calls atomic.Int64
	var in inbox
	_ = s.Subscribe(ctxT(t), topic, func(ctx context.Context, k json.RawMessage, ev cdc.ChangeEvent) error {
		if calls.Add(1) < 3 {
			return errors.New("timed out")
		}
		return in.handle(ctx, k, ev)
	})
	_ = producer(t).Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "x"))
	in.wait(t, 1)
	if n := countFor(t, topic+".dlq", 2*time.Second); n != 0 {
		t.Fatalf("%d records on the DLQ, want none", n)
	}
}
