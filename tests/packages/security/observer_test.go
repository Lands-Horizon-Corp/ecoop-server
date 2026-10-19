package security_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

// No build tag: the security service needs no infrastructure.

func observedSecurity(t testing.TB) (security.ISecurityService, *observability.Service) {
	t.Helper()
	o := obstest.New(t)
	return security.NewSecurityService("observer-secret", 8*1024, 1, 2, 16, 32, security.WithObserver(o)), o
}

func secCount(o *observability.Service, op, outcome string) float64 {
	return obstest.Count(o, "security", op, outcome)
}

// A wrong password is counted on its own, and never as an error: a guessing attack
// must be visible without raising the service's error rate.
func TestObserver_AWrongPasswordIsSkippedNotAnError(t *testing.T) {
	t.Parallel()
	s, o := observedSecurity(t)
	ctx := context.Background()
	hash, err := s.GeneratePassword(ctx, "correct horse")
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.VerifyPassword(ctx, hash, "correct horse"); err != nil || !ok {
		t.Fatalf("right password = %v, %v", ok, err)
	}
	for range 3 {
		if ok, err := s.VerifyPassword(ctx, hash, "wrong"); err != nil || ok {
			t.Fatalf("wrong password = %v, %v", ok, err)
		}
	}
	if got := secCount(o, "verify_password", "success"); got != 1 {
		t.Fatalf("verify success = %v, want 1", got)
	}
	if got := secCount(o, "verify_password", "skipped"); got != 3 {
		t.Fatalf("verify skipped = %v, want 3 wrong guesses", got)
	}
	if got := secCount(o, "verify_password", "error"); got != 0 {
		t.Fatalf("wrong passwords counted as %v errors", got)
	}
}

// A malformed hash or a tampered token is an error of the call.
func TestObserver_BadInputIsAnError(t *testing.T) {
	t.Parallel()
	s, o := observedSecurity(t)
	ctx := context.Background()
	if _, err := s.VerifyPassword(ctx, "not a hash", "x"); !errors.Is(err, security.ErrInvalidHash) {
		t.Fatalf("VerifyPassword = %v, want ErrInvalidHash", err)
	}
	if _, err := s.Decrypt(ctx, "v4.local.garbage"); err == nil {
		t.Fatal("a garbage token decrypted")
	}
	if _, err := s.DecryptData(ctx, []byte("short")); !errors.Is(err, security.ErrDecrypt) {
		t.Fatalf("DecryptData = %v, want ErrDecrypt", err)
	}
	for _, op := range []string{"verify_password", "decrypt", "decrypt_data"} {
		if got := secCount(o, op, "error"); got != 1 {
			t.Errorf("%s error = %v, want 1", op, got)
		}
	}
}

// Hashing, encrypting and decrypting each count as one operation.
func TestObserver_CryptoCallsAreOperations(t *testing.T) {
	t.Parallel()
	s, o := observedSecurity(t)
	ctx := context.Background()
	if _, err := s.GeneratePassword(ctx, "pw"); err != nil {
		t.Fatal(err)
	}
	tok, err := s.Encrypt(ctx, "payload", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Decrypt(ctx, tok); err != nil || got != "payload" {
		t.Fatalf("Decrypt = %q, %v", got, err)
	}
	ct, err := s.EncryptData(ctx, []byte("bytes"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.DecryptData(ctx, ct); err != nil || string(got) != "bytes" {
		t.Fatalf("DecryptData = %q, %v", got, err)
	}
	for _, op := range []string{"hash_password", "encrypt", "decrypt", "encrypt_data", "decrypt_data"} {
		if got := secCount(o, op, "success"); got != 1 {
			t.Errorf("%s success = %v, want 1", op, got)
		}
	}
}

// What is recorded is the operation and its outcome only: no secret, password,
// hash or token appears anywhere in the registry.
func TestObserver_NothingSecretIsRecorded(t *testing.T) {
	t.Parallel()
	s, o := observedSecurity(t)
	ctx := context.Background()
	const password = "S3cr3t-Pa55word-Value"
	hash, err := s.GeneratePassword(ctx, password)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.VerifyPassword(ctx, hash, password)
	tok, _ := s.Encrypt(ctx, "S3cr3t-Payload-Value", time.Minute)
	_, _ = s.Decrypt(ctx, tok)
	for _, sm := range o.Snapshot() {
		blob := sm.Name
		for k, v := range sm.Attrs {
			blob += " " + k + "=" + v
		}
		for _, secret := range []string{password, "S3cr3t-Payload-Value", hash, tok, "observer-secret"} {
			if strings.Contains(blob, secret) {
				t.Fatalf("a secret leaked into the registry: %s", blob)
			}
		}
	}
}

// No observer, and a bare struct, are fine.
func TestObserver_NoneIsFine(t *testing.T) {
	t.Parallel()
	s := newService(t)
	hash, err := s.GeneratePassword(context.Background(), "pw")
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.VerifyPassword(context.Background(), hash, "pw"); err != nil || !ok {
		t.Fatalf("VerifyPassword = %v, %v", ok, err)
	}
	var zero security.SecurityService
	if _, err := zero.GeneratePassword(context.Background(), "pw"); err == nil {
		t.Fatal("a zero service must return an error, not panic")
	}
}
