package otp_test

import (
	"context"
	"testing"
)

// Smoke: start, generate, verify, stop.
func TestSmoke(t *testing.T) {
	o := newOTP(t)
	ctx := ctxT(t)
	k := key(t)
	code, err := o.Generate(ctx, k)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	ok, err := o.Verify(ctx, k, code)
	if err != nil || !ok {
		t.Fatalf("Verify = %v, %v; want true, nil", ok, err)
	}
	if err := o.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
