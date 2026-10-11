package security_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
)

// Chaos: randomized garbage, bit flips and disrupted contexts against each operation.

func chaosRand(t *testing.T) *rand.Rand {
	t.Helper()
	seed := uint64(time.Now().UnixNano())
	t.Logf("chaos seed: %d", seed)
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}

func randBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.UintN(256))
	}
	return b
}

func TestChaos_EveryBitFlipInCiphertextIsDetected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	plain := []byte("ledger entry: +1,000.00")
	ct, _ := svc.EncryptData(ctx, plain)

	for i := range ct {
		for bit := range 8 {
			mutated := append([]byte(nil), ct...)
			mutated[i] ^= 1 << bit
			if _, err := svc.DecryptData(ctx, mutated); !errors.Is(err, security.ErrDecrypt) {
				t.Fatalf("flip byte %d bit %d: err = %v, want ErrDecrypt", i, bit, err)
			}
		}
	}
}

func TestChaos_RandomTokenCorruptionIsDetected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	r := chaosRand(t)
	tok, _ := svc.Encrypt(ctx, "chaos", time.Minute)
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

	for range 500 {
		b := []byte(tok)
		i := len("v4.local.") + r.IntN(len(b)-len("v4.local."))
		orig := b[i]
		for b[i] == orig {
			b[i] = alphabet[r.IntN(len(alphabet))]
		}
		if got, err := svc.Decrypt(ctx, string(b)); err == nil {
			t.Fatalf("corrupted token at %d accepted, payload %q", i, got)
		}
	}
}

func TestChaos_RandomGarbageNeverPanics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	r := chaosRand(t)

	for range 2000 {
		garbage := randBytes(r, r.IntN(256))
		s := string(garbage)
		mustNotPanic(t, "VerifyPassword", func() { _, _ = svc.VerifyPassword(ctx, s, s) })
		mustNotPanic(t, "Decrypt", func() { _, _ = svc.Decrypt(ctx, s) })
		mustNotPanic(t, "DecryptData", func() {
			if _, err := svc.DecryptData(ctx, garbage); err == nil {
				t.Fatalf("random bytes decrypted: %x", garbage)
			}
		})
	}
}

func TestChaos_StructuredGarbageHashes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	r := chaosRand(t)
	pieces := []string{"$", "argon2id", "v=19", "m=", "t=", "p=", ",", "0", "1", "-1", "65536", "99999999999", "AAAA", "====", "", "\x00", "💥"}

	for range 2000 {
		var sb strings.Builder
		for range r.IntN(20) {
			sb.WriteString(pieces[r.IntN(len(pieces))])
		}
		h := sb.String()
		start := time.Now()
		mustNotPanic(t, "VerifyPassword", func() {
			if ok, _ := svc.VerifyPassword(ctx, h, "pw"); ok {
				t.Fatalf("garbage hash %q verified", h)
			}
		})
		if d := time.Since(start); d > time.Second {
			t.Fatalf("garbage hash %q took %v", h, d)
		}
	}
}

func TestChaos_RandomPayloadsRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	r := chaosRand(t)

	for range 300 {
		data := randBytes(r, r.IntN(4096))
		ct, err := svc.EncryptData(ctx, data)
		if err != nil {
			t.Fatal(err)
		}
		pt, err := svc.DecryptData(ctx, ct)
		if err != nil || !bytes.Equal(pt, data) {
			t.Fatalf("round trip failed for %d bytes: %v", len(data), err)
		}

		s := strings.ToValidUTF8(string(data), "?")
		tok, err := svc.Encrypt(ctx, s, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := svc.Decrypt(ctx, tok); err != nil || got != s {
			t.Fatalf("token round trip failed: %v", err)
		}
	}
}

func TestChaos_CancelledAndExpiredContexts(t *testing.T) {
	t.Parallel()
	svc := newService(t)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
	defer cancel2()

	for name, ctx := range map[string]context.Context{"cancelled": cancelled, "deadline exceeded": expired} {
		t.Run(name, func(t *testing.T) {
			// The service does not observe ctx today; it must at least stay
			// correct (no panic, no corrupted output) when handed a dead one.
			mustNotPanic(t, "all ops", func() {
				hash, err := svc.GeneratePassword(ctx, "pw")
				if err == nil {
					if ok, _ := svc.VerifyPassword(ctx, hash, "pw"); !ok {
						t.Error("hash produced under dead ctx does not verify")
					}
				}
				if tok, err := svc.Encrypt(ctx, "x", time.Minute); err == nil {
					if got, _ := svc.Decrypt(ctx, tok); got != "x" {
						t.Error("token produced under dead ctx does not decrypt")
					}
				}
				if ct, err := svc.EncryptData(ctx, []byte("x")); err == nil {
					if pt, _ := svc.DecryptData(ctx, ct); string(pt) != "x" {
						t.Error("data produced under dead ctx does not decrypt")
					}
				}
			})
		})
	}
}
