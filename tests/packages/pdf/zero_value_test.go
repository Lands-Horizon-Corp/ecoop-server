//go:build integration

package pdf_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Zero value & absence: a zero struct and a handle kept after Stop refuse
// work; a nil receiver panics.

func TestZeroValue_ZeroStructAndStoppedHandle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if err := (&pdf.PDFEngineService{}).Start(ctx); !errors.Is(err, pdf.ErrNoTracker) {
		t.Errorf("zero struct Start = %v", err)
	}
	if _, err := (&pdf.PDFEngineService{}).Generate(ctx, invoice()); !errors.Is(err, pdf.ErrNotStarted) {
		t.Errorf("zero struct Generate = %v", err)
	}
	s := started(t)
	_ = s.Stop(ctx)
	if _, err := s.Generate(ctxT(t), invoice()); !errors.Is(err, pdf.ErrNotStarted) {
		t.Errorf("handle kept after Stop = %v", err)
	}
}

func TestZeroValue_NilReceiver(t *testing.T) {
	t.Parallel()
	var ptr *pdf.PDFEngineService
	mustPanic(t, "nil pointer", func() { _ = ptr.Start(context.Background()) })
}
