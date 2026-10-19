//go:build integration

package otp_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/otp"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

func observedOTP(t testing.TB) (otp.IOTPService, *observability.Service) {
	t.Helper()
	o := obstest.New(t)
	svc := otp.NewOTPService(secret, newSecurity(), newCache(t), mockSMTP(t), mockSMS(t), nil, otp.WithObserver(o))
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("otp Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc, o
}

func otpCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "otp", op, outcome)
}

// A right code is a success; a wrong one is skipped, never an error, so a guessing
// attack is visible on its own and cannot raise the error rate.
func TestObserver_AWrongCodeIsSkippedNotAnError(t *testing.T) {
	svc, o := observedOTP(t)
	ctx := context.Background()
	k := key(t)
	code, err := svc.Generate(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if ok, err := svc.Verify(ctx, k, "000000"); err != nil || ok {
			if code == "000000" {
				t.Skip("the generated code happened to be 000000")
			}
			t.Fatalf("wrong code = %v, %v", ok, err)
		}
	}
	if ok, err := svc.Verify(ctx, k, code); err != nil || !ok {
		t.Fatalf("right code = %v, %v", ok, err)
	}
	if got := otpCount(o, "verify", "success"); got != 1 {
		t.Fatalf("verify success = %v, want 1", got)
	}
	if got := otpCount(o, "verify", "skipped"); got != 2 {
		t.Fatalf("verify skipped = %v, want 2 wrong guesses", got)
	}
	if got := otpCount(o, "verify", "error"); got != 0 {
		t.Fatalf("wrong codes counted as %v errors", got)
	}
	if got := otpCount(o, "generate", "success"); got != 1 {
		t.Fatalf("generate success = %v, want 1", got)
	}
}

// An expired or unknown code, and a code revoked after too many guesses, are also
// answers: the caller still gets the specific error, only the metric calls it skipped.
func TestObserver_ExpiredAndExhaustedCodesAreSkippedButStillReturnTheirError(t *testing.T) {
	svc, o := observedOTP(t)
	ctx := context.Background()
	if _, err := svc.Verify(ctx, key(t), "123456"); !errors.Is(err, otp.ErrCodeNotFound) {
		t.Fatalf("an unknown code = %v, want ErrCodeNotFound", err)
	}
	k := key(t)
	if _, err := svc.Generate(ctx, k); err != nil {
		t.Fatal(err)
	}
	var last error
	for range otp.MaxAttempts + 1 {
		_, last = svc.Verify(ctx, k, "999999")
	}
	if !errors.Is(last, otp.ErrTooManyAttempts) {
		t.Fatalf("after too many guesses = %v, want ErrTooManyAttempts", last)
	}
	if got := otpCount(o, "verify", "error"); got != 0 {
		t.Fatalf("expired and exhausted codes counted as %v errors", got)
	}
	if got := otpCount(o, "verify", "skipped"); got < 2 {
		t.Fatalf("verify skipped = %v, want at least 2", got)
	}
}

// An empty key is refused before any operation begins, so it is not counted.
func TestObserver_ARefusedKeyIsNotCounted(t *testing.T) {
	svc, o := observedOTP(t)
	if _, err := svc.Generate(context.Background(), ""); !errors.Is(err, otp.ErrEmptyKey) {
		t.Fatalf("Generate with an empty key = %v, want ErrEmptyKey", err)
	}
	if got := otpCount(o, "generate", "error") + otpCount(o, "generate", "success"); got != 0 {
		t.Fatalf("a refused call was counted %v times", got)
	}
}

// The code, the key and the hash never reach the registry.
func TestObserver_NoCodeOrKeyIsRecorded(t *testing.T) {
	svc, o := observedOTP(t)
	ctx := context.Background()
	k := key(t)
	code, err := svc.Generate(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = svc.Verify(ctx, k, code)
	for _, sm := range o.Snapshot() {
		blob := sm.Name
		for key, v := range sm.Attrs {
			blob += " " + key + "=" + v
		}
		if strings.Contains(blob, k) || strings.Contains(blob, "code:"+code) || strings.Contains(blob, secret) {
			t.Fatalf("a code, key or secret leaked into the registry: %s", blob)
		}
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	svc := newOTP(t)
	k := key(t)
	code, err := svc.Generate(context.Background(), k)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := svc.Verify(context.Background(), k, code); err != nil || !ok {
		t.Fatalf("Verify = %v, %v", ok, err)
	}
	var zero otp.OTPService
	if _, err := zero.Generate(context.Background(), "k"); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
