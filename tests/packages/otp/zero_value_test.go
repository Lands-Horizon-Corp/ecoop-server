//go:build integration

package otp_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/otp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

// Zero value & absence: zero structs, nil and typed-nil dependencies, empty
// inputs, nil contexts and nil receivers.

func TestZeroValue_ZeroStruct(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	o := &otp.OTPService{}
	mustNotPanic(t, "zero struct", func() {
		if err := o.Start(ctx); !errors.Is(err, otp.ErrNoSecret) || !errors.Is(err, otp.ErrNoCache) {
			t.Errorf("Start = %v", err)
		}
		if _, err := o.Generate(ctx, "k"); !errors.Is(err, otp.ErrNotStarted) {
			t.Errorf("Generate = %v", err)
		}
		if _, err := o.Verify(ctx, "k", "1"); !errors.Is(err, otp.ErrNotStarted) {
			t.Errorf("Verify = %v", err)
		}
		if err := o.Revoke(ctx, "k"); !errors.Is(err, otp.ErrNotStarted) {
			t.Errorf("Revoke = %v", err)
		}
		if err := o.Stop(ctx); err != nil {
			t.Errorf("Stop = %v", err)
		}
	})
}

func TestZeroValue_StopIsIdempotent(t *testing.T) {
	t.Parallel()
	o := newOTP(t)
	for range 3 {
		if err := o.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestZeroValue_TypedNilDependencies(t *testing.T) {
	t.Parallel()
	// A nil *CacheService (etc.) stored in an interface is not == nil. Start
	// must report it as missing, not crash calling Ping on it.
	var ch *cache.CacheService
	var sec *security.SecurityService
	var mail *smtp.SMTPService
	var text *sms.SMSService
	o := otp.NewOTPService(secret, sec, ch, mail, text, nil)
	var err error
	mustNotPanic(t, "Start with typed-nil dependencies", func() { err = o.Start(context.Background()) })
	if !errors.Is(err, otp.ErrNoCache) || !errors.Is(err, otp.ErrNoSecurity) {
		t.Fatalf("Start = %v, want the typed-nil dependencies reported missing", err)
	}
}

func TestZeroValue_EmptyInputs(t *testing.T) {
	t.Parallel()
	o := newOTP(t)
	ctx, k := ctxT(t), key(t)
	_, _ = o.Generate(ctx, k)
	if ok, err := o.Verify(ctx, k, ""); ok || err != nil {
		t.Fatalf("empty code = %v, %v; want false, nil", ok, err)
	}
	if _, err := o.GenerateSMTP(ctx, key(t), smtp.SMTPRequest{To: "a@ecoop.test"}); err != nil {
		t.Fatalf("GenerateSMTP with nil Vars and empty body: %v", err)
	}
	if _, err := o.GenerateSMS(ctx, key(t), sms.SMSRequest{To: "+639171234567", Body: "{{.Code}}"}); err != nil {
		t.Fatalf("GenerateSMS with nil Vars: %v", err)
	}
}

func TestZeroValue_NilContext(t *testing.T) {
	t.Parallel()
	// A nil ctx is a programming error: it must fail fast, not hang.
	o := newOTP(t)
	var nilCtx context.Context
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = capturePanic(func() { _, _ = o.Generate(nilCtx, "k") })
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second): // Argon2 runs first and is slow under parallel load
		t.Fatal("Generate(nil ctx) hung")
	}
}

// Dangling pointers: a nil *OTPService or nil interface is a wiring bug and
// must fail loudly.
func TestZeroValue_NilReceiver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var ptr *otp.OTPService
	var iface otp.OTPServices
	ops := map[string]func(otp.OTPServices){
		"Start":    func(o otp.OTPServices) { _ = o.Start(ctx) },
		"Stop":     func(o otp.OTPServices) { _ = o.Stop(ctx) },
		"Generate": func(o otp.OTPServices) { _, _ = o.Generate(ctx, "k") },
		"Verify":   func(o otp.OTPServices) { _, _ = o.Verify(ctx, "k", "1") },
		"Revoke":   func(o otp.OTPServices) { _ = o.Revoke(ctx, "k") },
	}
	for name, op := range ops {
		t.Run(name+"/nil pointer", func(t *testing.T) { mustPanic(t, name, func() { op(ptr) }) })
		t.Run(name+"/nil interface", func(t *testing.T) { mustPanic(t, name, func() { op(iface) }) })
	}
}
