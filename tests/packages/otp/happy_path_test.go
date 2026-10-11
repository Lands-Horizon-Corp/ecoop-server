package otp_test

import (
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/otp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

func TestHappyPath_CodeShape(t *testing.T) {
	o := newOTP(t)
	seen := map[string]bool{}
	for range 50 {
		code, err := o.Generate(ctxT(t), key(t))
		if err != nil {
			t.Fatal(err)
		}
		if len(code) != otp.CodeLength || strings.Trim(code, "0123456789") != "" {
			t.Fatalf("code %q is not %d digits", code, otp.CodeLength)
		}
		seen[code] = true
	}
	if len(seen) < 45 {
		t.Fatalf("only %d distinct codes in 50: not random enough", len(seen))
	}
}

func TestHappyPath_VerifyOnce(t *testing.T) {
	o := newOTP(t)
	ctx, k := ctxT(t), key(t)
	code, _ := o.Generate(ctx, k)
	if ok, err := o.Verify(ctx, k, code); !ok || err != nil {
		t.Fatalf("first Verify = %v, %v", ok, err)
	}
	if ok, err := o.Verify(ctx, k, code); ok || !errors.Is(err, otp.ErrCodeNotFound) {
		t.Fatalf("second Verify = %v, %v; want false, ErrCodeNotFound", ok, err)
	}
}

func TestHappyPath_RegenerateReplacesOldCode(t *testing.T) {
	o := newOTP(t)
	ctx, k := ctxT(t), key(t)
	old, _ := o.Generate(ctx, k)
	fresh, _ := o.Generate(ctx, k)
	if old != fresh {
		if ok, _ := o.Verify(ctx, k, old); ok {
			t.Fatal("old code still valid after regenerating")
		}
	}
	if ok, err := o.Verify(ctx, k, fresh); !ok || err != nil {
		t.Fatalf("new code Verify = %v, %v", ok, err)
	}
}

func TestHappyPath_RegenerateResetsAttempts(t *testing.T) {
	o := newOTP(t)
	ctx, k := ctxT(t), key(t)
	code, _ := o.Generate(ctx, k)
	for range otp.MaxAttempts - 1 {
		_, _ = o.Verify(ctx, k, otherCode(code))
	}
	code, _ = o.Generate(ctx, k)
	for range otp.MaxAttempts - 1 {
		_, _ = o.Verify(ctx, k, otherCode(code))
	}
	if ok, err := o.Verify(ctx, k, code); !ok || err != nil {
		t.Fatalf("Verify after regenerate = %v, %v; attempts were not reset", ok, err)
	}
}

func TestHappyPath_Revoke(t *testing.T) {
	o := newOTP(t)
	ctx, k := ctxT(t), key(t)
	code, _ := o.Generate(ctx, k)
	if err := o.Revoke(ctx, k); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Verify(ctx, k, code); !errors.Is(err, otp.ErrCodeNotFound) {
		t.Fatalf("Verify after Revoke = %v", err)
	}
	if err := o.Revoke(ctx, k); err != nil {
		t.Fatalf("revoking twice = %v", err)
	}
}

func TestHappyPath_EmailCarriesTheRawCode(t *testing.T) {
	ch := newCache(t)
	q := newQueue(t, ch)
	o := startOTP(t, secret, ch, realSMTP(t, q), mockSMS(t))
	ctx, k, to := ctxT(t), key(t), inbox(t)
	code, err := o.GenerateSMTP(ctx, k, smtp.SMTPRequest{
		To: to, Name: "Ana", Subject: "Your code",
		Body: "<p>Hi {{.Name}}, your code is <b>{{.Code}}</b> for {{.Branch}}.</p>",
		Vars: map[string]string{"Branch": "Cebu"},
	})
	if err != nil {
		t.Fatal(err)
	}
	html := emailHTML(t, to)
	for _, want := range []string{code, "Hi Ana", "Cebu"} {
		if !strings.Contains(html, want) {
			t.Fatalf("email missing %q:\n%s", want, html)
		}
	}
	if ok, err := o.Verify(ctx, k, code); !ok || err != nil {
		t.Fatalf("Verify emailed code = %v, %v", ok, err)
	}
}

func TestHappyPath_SMSCarriesTheRawCode(t *testing.T) {
	ch := newCache(t)
	q := newQueue(t, ch)
	text, twilio := realSMS(t, q)
	o := startOTP(t, secret, ch, mockSMTP(t), text)
	ctx, k := ctxT(t), key(t)
	to := "+639171234567"
	code, err := o.GenerateSMS(ctx, k, sms.SMSRequest{To: to, Body: "eCoop code: {{.Code}}"})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool { _, ok := twilio.last(to); return ok }, "SMS never reached Twilio")
	if body, _ := twilio.last(to); body != "eCoop code: "+code {
		t.Fatalf("SMS body = %q, want the raw code %q", body, code)
	}
	if ok, err := o.Verify(ctx, k, code); !ok || err != nil {
		t.Fatalf("Verify texted code = %v, %v", ok, err)
	}
}

func TestHappyPath_CallerVarsUntouched(t *testing.T) {
	o := newOTP(t)
	vars := map[string]string{"Branch": "Cebu"}
	before := maps.Clone(vars)
	if _, err := o.GenerateSMTP(ctxT(t), key(t), smtp.SMTPRequest{To: "a@ecoop.test", Body: "{{.Code}}", Vars: vars}); err != nil {
		t.Fatal(err)
	}
	if _, err := o.GenerateSMS(ctxT(t), key(t), sms.SMSRequest{To: "+639171234567", Body: "{{.Code}}", Vars: vars}); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(vars, before) {
		t.Fatalf("caller's Vars changed to %v", vars)
	}
}

func TestHappyPath_RestartKeepsCodes(t *testing.T) {
	// Codes live in the cache, not the process.
	o := newOTP(t)
	ctx, k := ctxT(t), key(t)
	code, _ := o.Generate(ctx, k)
	_ = o.Stop(ctx)
	if err := o.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if ok, err := o.Verify(ctx, k, code); !ok || err != nil {
		t.Fatalf("Verify after restart = %v, %v", ok, err)
	}
}
