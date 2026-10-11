package inflight_test

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Zero value & absence: the zero Inflight, nil receivers, nil contexts.

func TestZeroValue_ZeroInflightIsUsable(t *testing.T) {
	t.Parallel()
	var f utils.Inflight
	if f.Count() != 0 {
		t.Fatalf("Count = %d", f.Count())
	}
	if err := f.Wait(context.Background()); err != nil {
		t.Fatalf("idle Wait = %v", err)
	}
	done := f.Track()
	res := waitAsync(&f, context.Background())
	if !blocked(res, 20*time.Millisecond) {
		t.Fatal("Wait returned with an op in flight")
	}
	done()
	if err := recv(t, res, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestZeroValue_NewInflightStartsIdle(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	if f == nil {
		t.Fatal("NewInflight returned nil")
	}
	if f.Count() != 0 {
		t.Fatalf("Count = %d", f.Count())
	}
}

func TestZeroValue_NilContextWhenIdle(t *testing.T) {
	t.Parallel()
	f := utils.NewInflight()
	var nilCtx context.Context
	mustNotPanic(t, "Wait(nil) idle", func() {
		if err := f.Wait(nilCtx); err != nil {
			t.Fatalf("Wait(nil) idle = %v", err)
		}
	})
}

func TestZeroValue_NilContextWhenBusyIsAProgrammingError(t *testing.T) {
	t.Parallel()
	// Documents behaviour: with work in flight Wait needs ctx.Done(), so a
	// nil ctx panics rather than blocking forever unnoticed.
	f := utils.NewInflight()
	defer f.Track()()
	var nilCtx context.Context
	mustPanic(t, "Wait(nil) busy", func() { _ = f.Wait(nilCtx) })
}

// Dangling pointer: a nil *Inflight is a wiring bug (a service constructed
// without its tracker) and must fail loudly.
func TestZeroValue_NilReceiverFailsLoudly(t *testing.T) {
	t.Parallel()
	var f *utils.Inflight
	mustPanic(t, "Track", func() { f.Track() })
	mustPanic(t, "Count", func() { f.Count() })
	mustPanic(t, "Wait", func() { _ = f.Wait(context.Background()) })
}
