package qr_test

import (
	"context"
	"encoding/base64"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
)

// Chaos: randomized garbage, bit flips and dead contexts.

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

func randString(r *rand.Rand, n int) string {
	return strings.ToValidUTF8(string(randBytes(r, n)), "?")
}

func TestChaos_EveryBitFlipIsDetected(t *testing.T) {
	svc := newStarted(t)
	raw, _ := base64.StdEncoding.DecodeString(mustEncode(t, svc, &qr.QRData{Data: "loan:7", Type: "ledger"}))
	for i := range raw {
		for bit := range 8 {
			m := append([]byte(nil), raw...)
			m[i] ^= 1 << bit
			if out, err := svc.Decode(context.Background(), base64.StdEncoding.EncodeToString(m)); err == nil {
				t.Fatalf("flip byte %d bit %d accepted: %+v", i, bit, out)
			}
		}
	}
}

func TestChaos_RandomGarbageNeverPanics(t *testing.T) {
	svc := newStarted(t)
	r := chaosRand(t)
	for range 3000 {
		var in string
		if r.IntN(2) == 0 {
			in = base64.StdEncoding.EncodeToString(randBytes(r, r.IntN(512)))
		} else {
			in = string(randBytes(r, r.IntN(512)))
		}
		mustNotPanic(t, "Decode", func() {
			if out, err := svc.Decode(context.Background(), in); err == nil {
				t.Fatalf("random input decoded to %+v", out)
			}
		})
	}
}

func TestChaos_RandomDataRoundTrips(t *testing.T) {
	ctx := context.Background()
	svc := newStarted(t)
	r := chaosRand(t)
	for range 500 {
		in := qr.QRData{Data: randString(r, r.IntN(2048)), Type: randString(r, r.IntN(32))}
		enc := mustEncode(t, svc, &in)
		out, err := svc.Decode(ctx, enc)
		if err != nil || out != in {
			t.Fatalf("round trip failed (err %v)", err)
		}
	}
}

func TestChaos_RandomTransportDamage(t *testing.T) {
	ctx := context.Background()
	svc := newStarted(t)
	r := chaosRand(t)
	enc := mustEncode(t, svc, &qr.QRData{Data: "scan-me"})
	damage := []func(string) string{
		func(s string) string { return s[:r.IntN(len(s))] },                       // partial scan
		func(s string) string { return strings.ReplaceAll(s, "+", " ") },          // form-decoded '+'
		func(s string) string { i := r.IntN(len(s)); return s[:i] + s[i+1:] },     // dropped char
		func(s string) string { i := r.IntN(len(s)); return s[:i] + "A" + s[i:] }, // inserted char
		func(s string) string { return strings.ToUpper(s) },                       // case mangling
	}
	for range 500 {
		d := damage[r.IntN(len(damage))](enc)
		if d == enc {
			continue
		}
		mustNotPanic(t, "Decode", func() {
			if _, err := svc.Decode(ctx, d); err == nil {
				t.Fatalf("damaged payload accepted: %q", d)
			}
		})
	}
}

func TestChaos_DeadContexts(t *testing.T) {
	svc := newStarted(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
	defer cancel2()

	for name, ctx := range map[string]context.Context{"cancelled": cancelled, "deadline exceeded": expired} {
		t.Run(name, func(t *testing.T) {
			// ctx is not observed today; output must still be valid.
			mustNotPanic(t, "Encode/Decode", func() {
				in := qr.QRData{Data: "x"}
				enc, err := svc.Encode(ctx, &in)
				if err != nil {
					return
				}
				if out, err := svc.Decode(ctx, enc); err == nil && out != in {
					t.Fatalf("corrupted under dead ctx: %+v", out)
				}
			})
		})
	}
}
