//go:build integration

package broadcast_test

import (
	"context"
	"testing"
	"time"
)

// Smoke: start, publish, a browser receives it, stop.
func TestSmoke(t *testing.T) {
	t.Parallel()
	b := started(t)
	ch := channel()
	sub := connect(t)
	sub.subscribe(t, ch)
	if err := b.Publish(ctxT(t), ch, "loan.approved", map[string]any{"loan_id": 42}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := sub.expect(t, 1, 10*time.Second)[0]
	if got.Event != "loan.approved" || got.Channel != ch || got.Data != `{"loan_id":42}` {
		t.Fatalf("received %+v", got)
	}
	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
