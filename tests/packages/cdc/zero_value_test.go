//go:build integration

package cdc_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// A zero struct, a nil receiver, empty and absent values.
func TestZeroValue_Absence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	zero := &cdc.CDCService{}
	if err := zero.Run(ctx); !errors.Is(err, cdc.ErrNoService) {
		t.Errorf("zero Run = %v", err)
	}
	if err := zero.Subscribe(ctx, "t", func(context.Context, json.RawMessage, cdc.ChangeEvent) error { return nil }); !errors.Is(err, cdc.ErrNoService) {
		t.Errorf("zero Subscribe = %v", err)
	}
	if err := zero.Stop(ctx); err != nil {
		t.Errorf("zero Stop = %v", err)
	}
	var ptr *cdc.CDCService
	if err := ptr.Run(ctx); !errors.Is(err, cdc.ErrNoService) || ptr.Topic("a", "b") != "" || ptr.Stop(ctx) != nil {
		t.Error("nil receiver misbehaves")
	}
	// tombstone and absent before/after
	if ev, err := cdc.Decode(nil); ev != nil || err != nil {
		t.Errorf("tombstone = %v, %v", ev, err)
	}
	ev, err := cdc.Decode([]byte(`{"op":"c","after":null}`))
	if err != nil || string(ev.After) != "null" || len(ev.Before) != 0 || ev.Source.Table != "" {
		t.Errorf("absent fields: %+v, %v", ev, err)
	}
	mustPanic(t, "nil handler call", func() {
		var h cdc.Handler
		_ = h(ctx, nil, cdc.ChangeEvent{})
	})
}

// Stop while a handler is mid-retry neither hangs nor panics, and a second
// Stop is a no-op.
func TestZeroValue_StopDuringRetries(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	entered := make(chan struct{}, 1)
	_ = s.Subscribe(ctxT(t), topic, func(context.Context, json.RawMessage, cdc.ChangeEvent) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		return errors.New("always fails")
	})
	_ = producer(t).Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "x"))
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("handler never ran")
	}
	for range 2 {
		if err := s.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
