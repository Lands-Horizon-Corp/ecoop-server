//go:build integration

package pdf_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Zero value & absence: zero structs, empty options, handles kept after
// Stop, nil contexts and nil receivers.

func TestZeroValue_ZeroStruct(t *testing.T) {
	t.Parallel()
	s := &pdf.PDFEngineService{}
	ctx := context.Background()
	mustNotPanic(t, "zero struct", func() {
		if err := s.Start(ctx); !errors.Is(err, pdf.ErrNoTracker) {
			t.Errorf("Start = %v", err)
		}
		_, e1 := s.Generate(ctx, invoice())
		_, e2 := s.GenerateToS3(ctx, invoice())
		for name, err := range map[string]error{"Ping": s.Ping(ctx), "Generate": e1, "GenerateToS3": e2} {
			if !errors.Is(err, pdf.ErrNotStarted) {
				t.Errorf("%s = %v", name, err)
			}
		}
		if err := s.Stop(ctx); err != nil {
			t.Errorf("Stop = %v", err)
		}
	})
}

func TestZeroValue_EmptyOptions(t *testing.T) {
	t.Parallel()
	s := started(t)
	if _, err := s.Generate(ctxT(t), pdf.RenderOptions{}); !errors.Is(err, pdf.ErrNoTemplate) {
		t.Fatalf("empty options = %v", err)
	}
	render(t, s, pdf.RenderOptions{Template: "<p>{{ missing }}</p>", Data: map[string]any{}}) // missing values render empty
}

func TestZeroValue_HandleAfterStop(t *testing.T) {
	t.Parallel()
	s := started(t)
	kept := s
	_ = s.Stop(context.Background())
	if _, err := kept.Generate(ctxT(t), invoice()); !errors.Is(err, pdf.ErrNotStarted) {
		t.Fatalf("render through a handle kept after Stop = %v", err)
	}
}

func TestZeroValue_NilContextFailsFast(t *testing.T) {
	t.Parallel()
	s := started(t)
	var nilCtx context.Context
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = capturePanic(func() { _, _ = s.Generate(nilCtx, invoice()) })
		_, _ = capturePanic(func() { _ = s.Ping(nilCtx) })
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("a nil context hung")
	}
}

func TestZeroValue_NilReceiver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var ptr *pdf.PDFEngineService
	var iface pdf.PDFServices
	ops := map[string]func(pdf.PDFServices){
		"Start":    func(s pdf.PDFServices) { _ = s.Start(ctx) },
		"Generate": func(s pdf.PDFServices) { _, _ = s.Generate(ctx, invoice()) },
	}
	for name, op := range ops {
		t.Run(name+"/nil pointer", func(t *testing.T) { mustPanic(t, name, func() { op(ptr) }) })
		t.Run(name+"/nil interface", func(t *testing.T) { mustPanic(t, name, func() { op(iface) }) })
	}
}
