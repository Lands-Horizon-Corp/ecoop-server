package qr_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
)

// Poison pill: inputs shaped to crash, stall, or corrupt the service.

func TestPoisonPill_HostileEncodedStrings(t *testing.T) {
	svc := newStarted(t)
	cases := map[string]string{
		"8MiB garbage":   strings.Repeat("A", 8<<20),
		"null bytes":     "\x00\x00\x00\x00",
		"unicode":        "🔥🔥🔥🔥",
		"only padding":   "====",
		"newlines":       "AAAA\nAAAA\n",
		"sql":            "' OR 1=1; --",
		"json injection": `{"data":"admin","type":"root"}`,
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			var err error
			mustNotPanic(t, "Decode", func() { _, err = svc.Decode(context.Background(), in) })
			if err == nil {
				t.Fatal("hostile input accepted")
			}
		})
	}
}

func TestPoisonPill_HostileFieldContentRoundTrips(t *testing.T) {
	ctx := context.Background()
	svc := newStarted(t)
	cases := map[string]qr.QRData{
		"null bytes":      {Data: "a\x00b", Type: "\x00"},
		"json breakout":   {Data: `","type":"admin`, Type: `"}`},
		"control chars":   {Data: "\x01\x02\x1b[31m", Type: "\x7f"},
		"unicode escapes": {Data: `\u0000‮`, Type: "‮"},
		"invalid utf8":    {Data: string([]byte{0xff, 0xfe, 0xc3, 0x28}), Type: "bin"},
		"lone surrogate":  {Data: string([]byte{0xed, 0xa0, 0x80}), Type: "bin"},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			enc, err := svc.Encode(ctx, &in)
			if err != nil {
				return // refusing is acceptable; silently altering is not
			}
			out, err := svc.Decode(ctx, enc)
			if err != nil || out != in {
				t.Fatalf("content altered in transit:\n in  %q\n out %q (err %v)", in.Data, out.Data, err)
			}
		})
	}
}

func TestPoisonPill_ForgedJSONIsHandledSafely(t *testing.T) {
	svc := newStarted(t)
	cases := map[string]string{
		"deep nesting":   `{"data":` + strings.Repeat(`[`, 10000) + strings.Repeat(`]`, 10000) + `}`,
		"duplicate keys": `{"data":"first","data":"second","type":"t"}`,
		"unknown fields": `{"data":"x","type":"y","admin":true,"__proto__":{}}`,
		"case folding":   `{"DATA":"x","Type":"y"}`,
		"huge number":    `{"data":"x","type":"y","n":1e999999}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			mustNotPanic(t, "Decode", func() {
				_, _ = svc.Decode(context.Background(), forge(t, testSecret, []byte(body), true))
			})
		})
	}
}

// Even with a leaked secret, a tiny QR must not be able to expand into a
// huge allocation (decompression bomb).
func TestPoisonPill_DecompressionBomb(t *testing.T) {
	svc := newStarted(t)
	// Valid JSON, so the only thing that can stop it is a size limit on
	// decompression (a JSON error would mask the 64MiB expansion).
	body := append([]byte(`{"type":"bomb","data":"`), bytes.Repeat([]byte{'a'}, 64<<20)...)
	bomb := forge(t, testSecret, append(body, `"}`...), true)
	if len(bomb) > 64<<10 {
		t.Fatalf("bomb unexpectedly large (%d bytes)", len(bomb))
	}
	start := time.Now()
	var err error
	mustNotPanic(t, "Decode", func() { _, err = svc.Decode(context.Background(), bomb) })
	if err == nil {
		t.Fatalf("%d-byte QR expanded to 64MiB without being rejected (took %v)", len(bomb), time.Since(start))
	}
}

func TestPoisonPill_ReplayIsNotDetected(t *testing.T) {
	// Documents current behaviour: QR codes carry no expiry or nonce
	// tracking, so the same code decodes forever. Callers must enforce
	// freshness themselves (e.g. a timestamp inside Data).
	ctx := context.Background()
	svc := newStarted(t)
	enc := mustEncode(t, svc, &qr.QRData{Data: "pay:100"})
	for range 3 {
		if _, err := svc.Decode(ctx, enc); err != nil {
			t.Fatalf("replay decode failed: %v", err)
		}
	}
	if _, err := base64.StdEncoding.DecodeString(enc); err != nil {
		t.Fatal(err)
	}
}
