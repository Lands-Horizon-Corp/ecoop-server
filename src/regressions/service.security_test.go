package regressions

import (
	"context"
	"testing"
	"time"

	"e-coop-server/pkg/services/security"
)

func newTestSecurityService() security.SecurityServices {
	return security.NewSecurityService(
		"test-secret-key-32-bytes-long!",
		8*1024, // 8MB memory for fast tests
		1,      // 1 iteration
		1,      // 1 parallelism
		16,     // 16 bytes salt
		32,     // 32 bytes key
	)
}

func TestSecurityService_Password(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	t.Run("Generate and Verify valid password", func(t *testing.T) {
		rawPassword := "SecureP@ssword123!"
		hash, err := sec.GeneratePassword(ctx, rawPassword)
		if err != nil {
			t.Fatalf("unexpected error generating password: %v", err)
		}
		if hash == "" {
			t.Fatal("expected non-empty hash")
		}

		valid, err := sec.VerifyPassword(ctx, hash, rawPassword)
		if err != nil {
			t.Fatalf("unexpected error verifying password: %v", err)
		}
		if !valid {
			t.Fatal("expected valid password verification to return true")
		}

		invalid, err := sec.VerifyPassword(ctx, hash, "WrongPassword!")
		if err != nil {
			t.Fatalf("unexpected error verifying wrong password: %v", err)
		}
		if invalid {
			t.Fatal("expected invalid password verification to return false")
		}
	})

	t.Run("Malformed hash verification", func(t *testing.T) {
		malformedHashes := []string{
			"invalid-hash",
			"$argon2id$v=19$m=65536,t=3,p=1$short",
			"$argon2id$v=19$invalid_params$salt$hash",
			"$argon2id$v=19$m=65536,t=3,p=1$invalid_b64!$invalid_b64!",
		}

		for _, h := range malformedHashes {
			valid, err := sec.VerifyPassword(ctx, h, "password")
			if valid || err == nil {
				t.Fatalf("expected error for malformed hash %q, got valid=%v, err=%v", h, valid, err)
			}
		}
	})
}

func TestSecurityService_EncryptDecrypt(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	t.Run("String payload roundtrip", func(t *testing.T) {
		plaintext := "Sensitive Cooperative User Data 123456"
		token, err := sec.Encrypt(ctx, plaintext, 5*time.Minute)
		if err != nil {
			t.Fatalf("encrypt failed: %v", err)
		}

		decrypted, err := sec.Decrypt(ctx, token)
		if err != nil {
			t.Fatalf("decrypt failed: %v", err)
		}
		if decrypted != plaintext {
			t.Fatalf("expected %q, got %q", plaintext, decrypted)
		}
	})

	t.Run("Expired token fails decryption", func(t *testing.T) {
		token, err := sec.Encrypt(ctx, "data", -1*time.Minute)
		if err != nil {
			t.Fatalf("encrypt failed: %v", err)
		}

		_, err = sec.Decrypt(ctx, token)
		if err == nil {
			t.Fatal("expected error for expired token, got nil")
		}
	})

	t.Run("Tampered token fails decryption", func(t *testing.T) {
		token, err := sec.Encrypt(ctx, "data", 5*time.Minute)
		if err != nil {
			t.Fatalf("encrypt failed: %v", err)
		}

		tampered := token[:len(token)-5] + "XXXXX"
		_, err = sec.Decrypt(ctx, tampered)
		if err == nil {
			t.Fatal("expected error for tampered token, got nil")
		}
	})
}

func BenchmarkSecurityService_EncryptDecrypt(b *testing.B) {
	sec := newTestSecurityService()
	ctx := context.Background()
	payload := "Benchmark payload test string"

	b.Run("Encrypt", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = sec.Encrypt(ctx, payload, 10*time.Minute)
		}
	})

	token, _ := sec.Encrypt(ctx, payload, 10*time.Minute)
	b.Run("Decrypt", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = sec.Decrypt(ctx, token)
		}
	})
}
