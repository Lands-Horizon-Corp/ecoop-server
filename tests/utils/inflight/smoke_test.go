package inflight_test

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Smoke: track one operation, finish it, drain.
func TestSmoke(t *testing.T) {
	f := utils.NewInflight()
	done := f.Track()
	if f.Count() != 1 {
		t.Fatalf("Count = %d, want 1", f.Count())
	}
	done()
	if err := f.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if f.Count() != 0 {
		t.Fatalf("Count = %d, want 0", f.Count())
	}
}
