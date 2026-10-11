package security_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/argon2"
)

func TestHappyPath_PasswordRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)

	cases := map[string]string{
		"ascii":      "correct horse battery staple",
		"unicode":    "pässwörd-日本語-🔐",
		"symbols":    `!@#$%^&*()_+-=[]{}|;':",./<>?` + "`~",
		"dollar":     "$argon2id$looks$like$a$hash",
		"whitespace": "  leading and trailing  ",
		"long":       strings.Repeat("a", 4096),
	}
	for name, pw := range cases {
		t.Run(name, func(t *testing.T) {
			hash, err := svc.GeneratePassword(ctx, pw)
			if err != nil {
				t.Fatalf("GeneratePassword: %v", err)
			}
			ok, err := svc.VerifyPassword(ctx, hash, pw)
			if err != nil || !ok {
				t.Fatalf("VerifyPassword = %v, %v; want true, nil", ok, err)
			}
		})
	}
}

func TestHappyPath_HashFormat(t *testing.T) {
	t.Parallel()
	svc := newService(t)
	hash, err := svc.GeneratePassword(context.Background(), "pw")
	if err != nil {
		t.Fatal(err)
	}

	parts := strings.Split(hash, "$")
	if len(parts) != 6 {
		t.Fatalf("hash has %d segments, want 6: %q", len(parts), hash)
	}
	if parts[1] != "argon2id" {
		t.Errorf("algorithm = %q, want argon2id", parts[1])
	}
	if want := fmt.Sprintf("v=%d", argon2.Version); parts[2] != want {
		t.Errorf("version = %q, want %q", parts[2], want)
	}
	if want := fmt.Sprintf("m=%d,t=%d,p=%d", testMemory, testIterations, testParallelism); parts[3] != want {
		t.Errorf("params = %q, want %q", parts[3], want)
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(salt) != testSalt {
		t.Errorf("salt len = %d (err %v), want %d", len(salt), err, testSalt)
	}
	key, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(key) != testKey {
		t.Errorf("key len = %d (err %v), want %d", len(key), err, testKey)
	}
}

func TestHappyPath_HashesAreSalted(t *testing.T) {
	t.Parallel()
	svc := newService(t)
	ctx := context.Background()
	a, _ := svc.GeneratePassword(ctx, "same")
	b, _ := svc.GeneratePassword(ctx, "same")
	if a == b {
		t.Fatal("two hashes of the same password are identical; salt is not random")
	}
}

func TestHappyPath_TokenRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)

	payloads := map[string]string{
		"plain":   "user:42",
		"json":    `{"id":42,"roles":["admin"]}`,
		"unicode": "héllo 世界 🚀",
		"large":   strings.Repeat("x", 64*1024),
	}
	for name, p := range payloads {
		t.Run(name, func(t *testing.T) {
			tok, err := svc.Encrypt(ctx, p, time.Hour)
			if err != nil {
				t.Fatalf("Encrypt: %v", err)
			}
			if !strings.HasPrefix(tok, "v4.local.") {
				t.Errorf("token %q is not a v4.local PASETO", tok[:min(len(tok), 16)])
			}
			got, err := svc.Decrypt(ctx, tok)
			if err != nil || got != p {
				t.Fatalf("Decrypt mismatch: err=%v", err)
			}
		})
	}
}

func TestHappyPath_TokenIsNotPlaintext(t *testing.T) {
	t.Parallel()
	svc := newService(t)
	tok, _ := svc.Encrypt(context.Background(), "super-secret-marker", time.Minute)
	if strings.Contains(tok, "super-secret-marker") {
		t.Fatal("payload is visible in token")
	}
}

func TestHappyPath_DataRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)

	inputs := map[string][]byte{
		"text":   []byte("account number 0001-2345"),
		"binary": {0x00, 0xff, 0x10, 0x00, 0x7f},
		"1MiB":   bytes.Repeat([]byte{0xab}, 1<<20),
	}
	for name, in := range inputs {
		t.Run(name, func(t *testing.T) {
			ct, err := svc.EncryptData(ctx, in)
			if err != nil {
				t.Fatalf("EncryptData: %v", err)
			}
			if bytes.Contains(ct, in) && len(in) > 4 {
				t.Error("ciphertext contains plaintext")
			}
			pt, err := svc.DecryptData(ctx, ct)
			if err != nil || !bytes.Equal(pt, in) {
				t.Fatalf("DecryptData mismatch: err=%v", err)
			}
		})
	}
}

func TestHappyPath_DataNonceIsRandom(t *testing.T) {
	t.Parallel()
	svc := newService(t)
	ctx := context.Background()
	a, _ := svc.EncryptData(ctx, []byte("same"))
	b, _ := svc.EncryptData(ctx, []byte("same"))
	if bytes.Equal(a, b) {
		t.Fatal("identical ciphertexts for identical plaintexts; nonce is not random")
	}
}

func TestHappyPath_SameSecretInteroperates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	a, b := newService(t), newService(t)

	tok, _ := a.Encrypt(ctx, "shared", time.Minute)
	if got, err := b.Decrypt(ctx, tok); err != nil || got != "shared" {
		t.Fatalf("token from A not readable by B: %q, %v", got, err)
	}
	ct, _ := a.EncryptData(ctx, []byte("shared"))
	if pt, err := b.DecryptData(ctx, ct); err != nil || string(pt) != "shared" {
		t.Fatalf("data from A not readable by B: %q, %v", pt, err)
	}
	hash, _ := a.GeneratePassword(ctx, "pw")
	if ok, err := b.VerifyPassword(ctx, hash, "pw"); err != nil || !ok {
		t.Fatalf("hash from A not verifiable by B: %v, %v", ok, err)
	}
}
