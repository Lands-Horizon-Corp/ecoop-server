//go:build integration

package pdf_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Chaos: the engine goes away and comes back.

func viaProxy(t *testing.T) (pdf.IPDFServices, *proxy) {
	t.Helper()
	p := newProxy(t)
	s := pdf.NewPDFService(p.URL(), token, nil)
	if err := s.Start(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s, p
}

func TestChaos_EngineGoneAndBack(t *testing.T) {
	t.Parallel()
	s, p := viaProxy(t)
	p.Down()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := s.Generate(ctx, invoice()); !errors.Is(err, pdf.ErrConnect) {
		t.Fatalf("engine gone: %v", err)
	}
	p.Up()
	render(t, s, invoice())
}
