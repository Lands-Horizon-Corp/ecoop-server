//go:build integration

package pdf_test

import (
	"io"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func observedPDF(t testing.TB) (pdf.IPDFServices, *observability.Service) {
	t.Helper()
	requireEngine(t)
	o := obstest.New(t)
	s := pdf.NewPDFService(engineURL(engineHost, enginePort), token, nil, pdf.WithObserver(o))
	if err := s.Start(ctxT(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(ctxT(t)) })
	return s, o
}

func pdfCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "pdf", op, outcome)
}

// Generating and pinging are each one counted, timed operation.
func TestObserver_GenerateAndPingAreOperations(t *testing.T) {
	s, o := observedPDF(t)
	body, err := s.Generate(ctxT(t), invoice())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
	if err := s.Ping(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	if got := pdfCount(o, "generate", "success"); got != 1 {
		t.Fatalf("generate success = %v, want 1", got)
	}
	if got := pdfCount(o, "ping", "success"); got != 1 {
		t.Fatalf("ping success = %v, want 1", got)
	}
}

// A request the engine refuses, or that is invalid, is an error.
func TestObserver_ARefusedRenderIsAnError(t *testing.T) {
	s, o := observedPDF(t)
	if _, err := s.Generate(ctxT(t), pdf.RenderOptions{}); err == nil { // no template
		t.Fatal("an empty render was accepted")
	}
	if got := pdfCount(o, "generate", "error"); got != 1 {
		t.Fatalf("generate error = %v, want 1", got)
	}
	if got := pdfCount(o, "generate", "success"); got != 0 {
		t.Fatalf("a refused render counted as success %v times", got)
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	s := started(t)
	body, err := s.Generate(ctxT(t), invoice())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
	var zero pdf.PDFEngineService
	if err := zero.Ping(ctxT(t)); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
