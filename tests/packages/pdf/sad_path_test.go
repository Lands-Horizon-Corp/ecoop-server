//go:build integration

package pdf_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Sad path: wrong settings and wrong requests are refused with the right
// error.

func TestSad_StartChecksSettingsAndTheEngine(t *testing.T) {
	t.Parallel()
	requireEngine(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := pdf.NewPDFEngine("", "", nil).Start(ctx)
	if !errors.Is(err, pdf.ErrNoURL) || !errors.Is(err, pdf.ErrNoToken) {
		t.Errorf("empty settings: %v", err)
	}
	if err := pdf.NewPDFEngine("http://127.0.0.1:1", token, nil).Start(ctx); !errors.Is(err, pdf.ErrConnect) {
		t.Errorf("closed port: %v", err)
	}
}

func TestSad_WrongTokenIsRefusedByTheEngine(t *testing.T) {
	t.Parallel()
	requireEngine(t)
	s := pdf.NewPDFEngine(engineURL(engineHost, enginePort), "not-the-token", nil)
	if err := s.Start(ctxT(t)); err != nil { // /health needs no token
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	if _, err := s.Generate(ctxT(t), invoice()); !errors.Is(err, pdf.ErrEngine) {
		t.Fatalf("wrong token: %v", err)
	}
}

func TestSad_CallsBeforeStartAndAfterStop(t *testing.T) {
	t.Parallel()
	requireEngine(t)
	s := pdf.NewPDFEngine(engineURL(engineHost, enginePort), token, nil)
	ctx := ctxT(t)
	check := func(when string) {
		t.Helper()
		_, e1 := s.Generate(ctx, invoice())
		_, e2 := s.GenerateToS3(ctx, invoice())
		for name, err := range map[string]error{"Ping": s.Ping(ctx), "Generate": e1, "GenerateToS3": e2} {
			if !errors.Is(err, pdf.ErrNotStarted) {
				t.Errorf("%s: %s = %v", when, name, err)
			}
		}
	}
	check("before Start")
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	_ = s.Stop(ctx)
	check("after Stop")
}

func TestSad_BadRequests(t *testing.T) {
	t.Parallel()
	s := started(t)
	ctx := ctxT(t)
	bad := func(edit func(*pdf.RenderOptions)) error {
		o := invoice()
		edit(&o)
		_, err := s.Generate(ctx, o)
		return err
	}
	for name, c := range map[string]struct{ err, want error }{
		"empty template":   {bad(func(o *pdf.RenderOptions) { o.Template = "  " }), pdf.ErrNoTemplate},
		"bad orientation":  {bad(func(o *pdf.RenderOptions) { o.Orientation = "sideways" }), pdf.ErrOrientation},
		"unencodable data": {bad(func(o *pdf.RenderOptions) { o.Data = map[string]any{"f": func() {}} }), pdf.ErrEncodeData},
		"bad width":        {bad(func(o *pdf.RenderOptions) { o.Width = "wide" }), pdf.ErrEngine},
		"broken template":  {bad(func(o *pdf.RenderOptions) { o.Template = "{% for x in %}" }), pdf.ErrEngine},
	} {
		if !errors.Is(c.err, c.want) {
			t.Errorf("%s: %v, want %v", name, c.err, c.want)
		}
	}
}
