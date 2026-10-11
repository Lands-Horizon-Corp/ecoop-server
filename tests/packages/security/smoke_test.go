package security_test

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// Smoke: every public operation works once with sane config.
func TestSmoke(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)

	t.Run("password", func(t *testing.T) {
		hash, err := svc.GeneratePassword(ctx, "hunter2")
		if err != nil {
			t.Fatalf("GeneratePassword: %v", err)
		}
		ok, err := svc.VerifyPassword(ctx, hash, "hunter2")
		if err != nil || !ok {
			t.Fatalf("VerifyPassword = %v, %v; want true, nil", ok, err)
		}
	})

	t.Run("token", func(t *testing.T) {
		tok, err := svc.Encrypt(ctx, "payload", time.Minute)
		if err != nil {
			t.Fatalf("Encrypt: %v", err)
		}
		got, err := svc.Decrypt(ctx, tok)
		if err != nil || got != "payload" {
			t.Fatalf("Decrypt = %q, %v; want %q, nil", got, err, "payload")
		}
	})

	t.Run("data", func(t *testing.T) {
		ct, err := svc.EncryptData(ctx, []byte("blob"))
		if err != nil {
			t.Fatalf("EncryptData: %v", err)
		}
		pt, err := svc.DecryptData(ctx, ct)
		if err != nil || !bytes.Equal(pt, []byte("blob")) {
			t.Fatalf("DecryptData = %q, %v; want %q, nil", pt, err, "blob")
		}
	})
}
