package security_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
)

// Zero value & absence: empty strings, nil slices, zero config, nil
// contexts, nil receivers, and slices that must not alias each other.

func TestZeroValue_ZeroConfigMustNotPanic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := map[string]security.SecurityServices{
		"all zero":        security.NewSecurityService("", 0, 0, 0, 0, 0),
		"zero iterations": security.NewSecurityService(testSecret, testMemory, 0, testParallelism, testSalt, testKey),
		"zero parallel":   security.NewSecurityService(testSecret, testMemory, testIterations, 0, testSalt, testKey),
		"zero key len":    security.NewSecurityService(testSecret, testMemory, testIterations, testParallelism, testSalt, 0),
		"zero struct":     &security.SecurityService{},
	}
	for name, svc := range cases {
		t.Run(name, func(t *testing.T) {
			var err error
			mustNotPanic(t, "GeneratePassword", func() { _, err = svc.GeneratePassword(ctx, "pw") })
			if err == nil {
				t.Error("GeneratePassword with invalid config returned no error")
			}
		})
	}
}

func TestZeroValue_ZeroSaltLengthStillSalts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := security.NewSecurityService(testSecret, testMemory, testIterations, testParallelism, 0, testKey)
	var a, b string
	var errA, errB error
	mustNotPanic(t, "GeneratePassword", func() {
		a, errA = svc.GeneratePassword(ctx, "pw")
		b, errB = svc.GeneratePassword(ctx, "pw")
	})
	if errA != nil || errB != nil {
		return // rejecting the config outright is acceptable
	}
	if a == b {
		t.Fatalf("salt length 0 yields deterministic unsalted hashes: %s", a)
	}
}

func TestZeroValue_ZeroMemoryIsClampedNotCrashed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := security.NewSecurityService(testSecret, 0, testIterations, testParallelism, testSalt, testKey)
	mustNotPanic(t, "zero memory", func() {
		hash, err := svc.GeneratePassword(ctx, "pw")
		if err != nil {
			return
		}
		if ok, err := svc.VerifyPassword(ctx, hash, "pw"); err != nil || !ok {
			t.Errorf("hash from zero-memory config did not verify: %v, %v", ok, err)
		}
	})
}

func TestZeroValue_EmptySecretStillWorks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newServiceWithSecret(t, "")
	tok, err := svc.Encrypt(ctx, "x", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := svc.Decrypt(ctx, tok); err != nil || got != "x" {
		t.Fatalf("Decrypt = %q, %v", got, err)
	}
	if _, err := newService(t).Decrypt(ctx, tok); err == nil {
		t.Fatal("empty-secret token accepted by a service with a real secret")
	}
}

func TestZeroValue_EmptyPassword(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	hash, err := svc.GeneratePassword(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := svc.VerifyPassword(ctx, hash, ""); !ok {
		t.Error("empty password did not verify against its own hash")
	}
	if ok, _ := svc.VerifyPassword(ctx, hash, " "); ok {
		t.Error("whitespace matched empty password")
	}
}

func TestZeroValue_EmptyHash(t *testing.T) {
	t.Parallel()
	svc := newService(t)
	for _, h := range []string{"", "$", "$$$$$", "$argon2id$v=19$m=8192,t=1,p=2$$"} {
		if ok, _ := svc.VerifyPassword(context.Background(), h, ""); ok {
			t.Errorf("empty-ish hash %q verified", h)
		}
	}
}

func TestZeroValue_EmptyPayloadAndToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	tok, err := svc.Encrypt(ctx, "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := svc.Decrypt(ctx, tok); err != nil || got != "" {
		t.Fatalf("empty payload round trip = %q, %v", got, err)
	}
	if _, err := svc.Decrypt(ctx, ""); err == nil {
		t.Fatal("empty token string accepted")
	}
}

func TestZeroValue_ZeroAndNegativeTTL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	for _, ttl := range []time.Duration{0, -time.Nanosecond, -time.Hour} {
		tok, err := svc.Encrypt(ctx, "x", ttl)
		if err != nil {
			continue
		}
		if _, err := svc.Decrypt(ctx, tok); err == nil {
			t.Errorf("token with ttl %v was accepted", ttl)
		}
	}
}

func TestZeroValue_NilAndEmptyData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	for name, in := range map[string][]byte{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			ct, err := svc.EncryptData(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			if len(ct) != 12+16 {
				t.Errorf("ciphertext len = %d, want nonce+tag (28)", len(ct))
			}
			pt, err := svc.DecryptData(ctx, ct)
			if err != nil || len(pt) != 0 {
				t.Fatalf("DecryptData = %q, %v; want empty, nil", pt, err)
			}
		})
	}
}

func TestZeroValue_NilAndEmptyCiphertext(t *testing.T) {
	t.Parallel()
	svc := newService(t)
	for name, in := range map[string][]byte{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			pt, err := svc.DecryptData(context.Background(), in)
			if !errors.Is(err, security.ErrDecrypt) || pt != nil {
				t.Fatalf("DecryptData = %v, %v; want nil, ErrDecrypt", pt, err)
			}
		})
	}
}

func TestZeroValue_NilContext(t *testing.T) {
	t.Parallel()
	svc := newService(t)
	var nilCtx context.Context // absent context
	mustNotPanic(t, "nil ctx", func() {
		hash, _ := svc.GeneratePassword(nilCtx, "pw")
		if ok, _ := svc.VerifyPassword(nilCtx, hash, "pw"); !ok {
			t.Error("VerifyPassword failed with nil ctx")
		}
		tok, _ := svc.Encrypt(nilCtx, "x", time.Minute)
		if got, _ := svc.Decrypt(nilCtx, tok); got != "x" {
			t.Error("Decrypt failed with nil ctx")
		}
		ct, _ := svc.EncryptData(nilCtx, []byte("x"))
		if pt, _ := svc.DecryptData(nilCtx, ct); string(pt) != "x" {
			t.Error("DecryptData failed with nil ctx")
		}
	})
}

// Dangling pointers: a nil *SecurityService (or nil interface) is a programming
// error; these pin down that it fails loudly rather than silently using zero keys.
func TestZeroValue_NilReceiverFailsLoudly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var ptr *security.SecurityService
	var iface security.SecurityServices

	ops := map[string]func(security.SecurityServices){
		"GeneratePassword": func(s security.SecurityServices) { _, _ = s.GeneratePassword(ctx, "pw") },
		"VerifyPassword": func(s security.SecurityServices) {
			_, _ = s.VerifyPassword(ctx, argonHash("argon2id", 19, 8192, 1, 1, "c2FsdA", "aGFzaA"), "pw")
		},
		"Encrypt":     func(s security.SecurityServices) { _, _ = s.Encrypt(ctx, "x", time.Minute) },
		"Decrypt":     func(s security.SecurityServices) { _, _ = s.Decrypt(ctx, "v4.local.AAAA") },
		"EncryptData": func(s security.SecurityServices) { _, _ = s.EncryptData(ctx, []byte("x")) },
		"DecryptData": func(s security.SecurityServices) { _, _ = s.DecryptData(ctx, make([]byte, 64)) },
	}
	for name, op := range ops {
		t.Run(name+"/nil pointer", func(t *testing.T) { mustPanic(t, name, func() { op(ptr) }) })
		t.Run(name+"/nil interface", func(t *testing.T) { mustPanic(t, name, func() { op(iface) }) })
	}
}

func TestZeroValue_OutputsDoNotAliasInputs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)

	in := []byte("original")
	ct, _ := svc.EncryptData(ctx, in)
	in[0] = 'X'
	if pt, err := svc.DecryptData(ctx, ct); err != nil || string(pt) != "original" {
		t.Fatalf("ciphertext changed after mutating plaintext input: %q %v", pt, err)
	}

	ctCopy := append([]byte(nil), ct...)
	pt, _ := svc.DecryptData(ctx, ct)
	pt[0] = 'Z'
	if !bytes.Equal(ct, ctCopy) {
		t.Fatal("mutating decrypted plaintext changed the ciphertext buffer")
	}
	if pt2, _ := svc.DecryptData(ctx, ct); string(pt2) != "original" {
		t.Fatalf("second decrypt saw mutation: %q", pt2)
	}

	if strings.Contains(string(ct), "original") {
		t.Fatal("plaintext leaked into ciphertext")
	}
}
