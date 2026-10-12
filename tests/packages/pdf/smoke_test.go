//go:build integration

package pdf_test

import (
	"context"
	"testing"
)

// Smoke: start, render an invoice, stop.
func TestSmoke(t *testing.T) {
	t.Parallel()
	s := started(t)
	render(t, s, invoice())
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
