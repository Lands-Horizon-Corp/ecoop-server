package qr_test

import (
	"context"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
)

// Smoke: the full lifecycle works once with sane input.
func TestSmoke(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := qr.NewQRServices(testSecret, nil)
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	in := qr.QRData{Data: "member:42", Type: "member"}
	enc, err := svc.Encode(ctx, &in)
	if err != nil || enc == "" {
		t.Fatalf("Encode = %q, %v", enc, err)
	}
	out, err := svc.Decode(ctx, enc)
	if err != nil || out != in {
		t.Fatalf("Decode = %+v, %v; want %+v", out, err, in)
	}
	if err := svc.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
