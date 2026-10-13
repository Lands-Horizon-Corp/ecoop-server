package qr_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"runtime/debug"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/crypto/chacha20poly1305"
)

const testSecret = "ecoop-qr-test-secret"

var _ qr.IQRServices = (*qr.QRService)(nil)

func newStarted(t testing.TB) qr.IQRServices {
	t.Helper()
	return newStartedWithSecret(t, testSecret)
}

func newStartedWithSecret(t testing.TB, secret string) qr.IQRServices {
	t.Helper()
	svc := qr.NewQRServices(secret, nil)
	if svc == nil {
		t.Fatal("NewQRServices returned nil")
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc
}

func mustEncode(t testing.TB, svc qr.IQRServices, d *qr.QRData) string {
	t.Helper()
	s, err := svc.Encode(context.Background(), d)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return s
}

// forge builds a payload exactly the way the service does, so tests can feed
// Decode authentic-but-hostile plaintext as if the secret had leaked.
func forge(t testing.TB, secret string, plaintext []byte, compress bool) string {
	t.Helper()
	body := plaintext
	if compress {
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			t.Fatal(err)
		}
		body = enc.EncodeAll(plaintext, nil)
		_ = enc.Close()
	}
	key := sha256.Sum256([]byte(secret))
	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(aead.Seal(nonce, nonce, body, nil))
}

func capturePanic(fn func()) (recovered any, stack string) {
	defer func() {
		if r := recover(); r != nil {
			recovered, stack = r, string(debug.Stack())
		}
	}()
	fn()
	return nil, ""
}

func mustNotPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	if r, stack := capturePanic(fn); r != nil {
		if testing.Verbose() {
			t.Log(stack)
		}
		t.Fatalf("%s panicked: %v", name, r)
	}
}

func mustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	if r, _ := capturePanic(fn); r == nil {
		t.Fatalf("%s: expected panic, got none", name)
	}
}
