package security_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
)

func TestSadPath_WrongPassword(t *testing.T) {
	ctx := context.Background()
	svc := newService(t)
	hash, _ := svc.GeneratePassword(ctx, "right")

	for _, wrong := range []string{"wrong", "Right", "right ", " right", "righ", ""} {
		ok, err := svc.VerifyPassword(ctx, hash, wrong)
		if err != nil {
			t.Errorf("VerifyPassword(%q) err = %v, want nil", wrong, err)
		}
		if ok {
			t.Errorf("VerifyPassword(%q) = true, want false", wrong)
		}
	}
}

func TestSadPath_MalformedHash(t *testing.T) {
	svc := newService(t)
	cases := map[string]string{
		"empty":           "",
		"not a hash":      "plaintext",
		"too few parts":   "$argon2id$v=19$m=8192,t=1,p=2$c2FsdA",
		"too many parts":  "$argon2id$v=19$m=8192,t=1,p=2$c2FsdA$aGFzaA$extra",
		"bad params":      "$argon2id$v=19$memory=8192$c2FsdA$aGFzaA",
		"zero iterations": argonHash("argon2id", 19, 8192, 0, 2, "c2FsdA", "aGFzaA"),
		"zero parallel":   argonHash("argon2id", 19, 8192, 1, 0, "c2FsdA", "aGFzaA"),
		"bad salt b64":    argonHash("argon2id", 19, 8192, 1, 2, "!!!!", "aGFzaA"),
		"bad hash b64":    argonHash("argon2id", 19, 8192, 1, 2, "c2FsdA", "!!!!"),
		"padded b64 salt": argonHash("argon2id", 19, 8192, 1, 2, "c2FsdA==", "aGFzaA"),
	}
	for name, hash := range cases {
		t.Run(name, func(t *testing.T) {
			ok, err := svc.VerifyPassword(context.Background(), hash, "pw")
			if err == nil {
				t.Errorf("err = nil, want error")
			}
			if ok {
				t.Errorf("ok = true, want false")
			}
		})
	}
}

func TestSadPath_HashParamsOverLimits(t *testing.T) {
	svc := newService(t)
	cases := map[string]string{
		"memory":      argonHash("argon2id", 19, 256*1024+1, 1, 1, "c2FsdA", "aGFzaA"),
		"iterations":  argonHash("argon2id", 19, 8192, 17, 1, "c2FsdA", "aGFzaA"),
		"parallelism": argonHash("argon2id", 19, 8192, 1, 65, "c2FsdA", "aGFzaA"),
	}
	for name, hash := range cases {
		t.Run(name, func(t *testing.T) {
			if ok, err := svc.VerifyPassword(context.Background(), hash, "pw"); err == nil || ok {
				t.Fatalf("VerifyPassword = %v, %v; want false, error", ok, err)
			}
		})
	}
}

func TestSadPath_ExpiredToken(t *testing.T) {
	ctx := context.Background()
	svc := newService(t)
	tok, err := svc.Encrypt(ctx, "x", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2100 * time.Millisecond) // PASETO timestamps have second precision
	if _, err := svc.Decrypt(ctx, tok); err == nil {
		t.Fatal("expired token was accepted")
	}
}

func TestSadPath_TokenFromOtherSecret(t *testing.T) {
	ctx := context.Background()
	tok, _ := newServiceWithSecret(t, "secret-a").Encrypt(ctx, "x", time.Minute)
	if _, err := newServiceWithSecret(t, "secret-b").Decrypt(ctx, tok); err == nil {
		t.Fatal("token decrypted with the wrong secret")
	}
}

func TestSadPath_TamperedToken(t *testing.T) {
	ctx := context.Background()
	svc := newService(t)
	tok, _ := svc.Encrypt(ctx, "x", time.Minute)

	b := []byte(tok)
	i := len("v4.local.") + 10
	if b[i] == 'A' {
		b[i] = 'B'
	} else {
		b[i] = 'A'
	}
	if _, err := svc.Decrypt(ctx, string(b)); err == nil {
		t.Fatal("tampered token accepted")
	}
	if _, err := svc.Decrypt(ctx, tok[:len(tok)-4]); err == nil {
		t.Fatal("truncated token accepted")
	}
	if _, err := svc.Decrypt(ctx, strings.Replace(tok, "v4.local.", "v4.public.", 1)); err == nil {
		t.Fatal("token with swapped purpose accepted")
	}
}

func TestSadPath_DataWrongSecret(t *testing.T) {
	ctx := context.Background()
	ct, _ := newServiceWithSecret(t, "secret-a").EncryptData(ctx, []byte("x"))
	_, err := newServiceWithSecret(t, "secret-b").DecryptData(ctx, ct)
	if !errors.Is(err, security.ErrDecrypt) {
		t.Fatalf("err = %v, want ErrDecrypt", err)
	}
}

func TestSadPath_DataTamperedOrShort(t *testing.T) {
	ctx := context.Background()
	svc := newService(t)
	ct, _ := svc.EncryptData(ctx, []byte("payload"))

	tampered := append([]byte(nil), ct...)
	tampered[len(tampered)-1] ^= 0x01

	cases := map[string][]byte{
		"tampered tag":     tampered,
		"truncated":        ct[:len(ct)-1],
		"nonce only":       ct[:12],
		"under minimum":    ct[:12+16-1],
		"extra byte":       append(append([]byte(nil), ct...), 0x00),
		"nonce+tag zeroes": make([]byte, 12+16),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			pt, err := svc.DecryptData(ctx, in)
			if !errors.Is(err, security.ErrDecrypt) {
				t.Fatalf("err = %v, want ErrDecrypt", err)
			}
			if pt != nil {
				t.Fatalf("plaintext = %q, want nil on failure", pt)
			}
		})
	}
}
