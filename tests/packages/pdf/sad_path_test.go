//go:build integration

package pdf_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Sad path: bad settings and bad requests get the right error.

func TestSad_Settings(t *testing.T) {
	t.Parallel()
	requireEngine(t)
	ctx := ctxT(t)
	if err := pdf.NewPDFService("", "", nil).Start(ctx); !errors.Is(err, pdf.ErrNoURL) || !errors.Is(err, pdf.ErrNoToken) {
		t.Errorf("empty settings: %v", err)
	}
	wrongToken := pdf.NewPDFService(engineURL(engineHost, enginePort), "not-the-token", nil)
	if err := wrongToken.Start(ctx); err != nil { // /health needs no token
		t.Fatal(err)
	}
	defer wrongToken.Stop(context.Background())
	if _, err := wrongToken.Generate(ctx, invoice()); !errors.Is(err, pdf.ErrEngine) {
		t.Errorf("wrong token: %v", err)
	}
}

func TestSad_Requests(t *testing.T) {
	t.Parallel()
	s := started(t)
	for want, opts := range map[error]pdf.RenderOptions{
		pdf.ErrNoTemplate:  {},
		pdf.ErrOrientation: {Template: "<p>x</p>", Orientation: "sideways"},
		pdf.ErrEncodeData:  {Template: "<p>x</p>", Data: func() {}},
		pdf.ErrEngine:      {Template: "{% for x in %}"},
	} {
		if _, err := s.Generate(ctxT(t), opts); !errors.Is(err, want) {
			t.Errorf("%+v: %v, want %v", opts, err, want)
		}
	}
}
