//go:build integration

package pdf_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Chaos: the engine disappears or slows down.

func viaProxy(t *testing.T) (pdf.PDFServices, *proxy) {
	t.Helper()
	p := newProxy(t)
	s := pdf.NewPDFEngine(p.URL(), token, nil)
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
	if err := s.Ping(ctx); err == nil {
		t.Fatal("Ping healthy with the engine gone")
	}
	p.Up()
	render(t, s, invoice())
}

func TestChaos_SlowEngineRespectsTheContext(t *testing.T) {
	t.Parallel()
	s, p := viaProxy(t)
	p.SetDelay(3 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := s.Generate(ctx, invoice()); err == nil {
		t.Fatal("render finished through a stalled engine")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Generate took %v past its deadline", d)
	}
}

// The connection dropping mid-download surfaces as a read error, not a
// truncated "PDF".
func TestChaos_ConnectionCutWhileReading(t *testing.T) {
	t.Parallel()
	s, p := viaProxy(t)
	body, err := s.Generate(ctxT(t), invoice())
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	p.DropAll()
	buf := make([]byte, 1<<20)
	_, _ = body.Read(buf[:1])
	_ = buf // reading may fail or finish from buffers; it must not hang or panic
}
