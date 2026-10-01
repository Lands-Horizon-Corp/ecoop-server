package regressions

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/qr"
	qrService "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/qr"
)

// Regression guards for qr.QRService.
//
// History: Encode/Decode used to build a new zstd encoder/decoder (and AEAD)
// on every call, costing ~4ms per round trip and leaking goroutines because
// the encoder was never closed. These tests fail if that comes back.
//
// Budgets are ~10x looser than measured values so they only trip on real
// regressions, not on a slow CI box. Measured: ~26µs and 12 allocs per round trip.

const qrSecret = "regression-secret-key"

func sampleQRData() qr.QRData {
	return qr.QRData{
		Data: strings.Repeat("ticket_id:TKT-98421049-X7 event:EVT-2026-SUMMIT-01 holder:Jane Doe; ", 15),
		Type: "ticket",
	}
}

func newQR(t testing.TB) qr.QRServices {
	t.Helper()
	svc := qrService.NewQRService(qrSecret)
	return svc
}

func TestQRRoundTripKeepsData(t *testing.T) {
	ctx := context.Background()
	qr := newQR(t)
	want := sampleQRData()

	enc, err := qr.Encode(ctx, &want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := qr.Decode(ctx, enc)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("round trip changed data:\n got %+v\nwant %+v", got, want)
	}
}

// Every value must survive Encode -> Decode byte-for-byte, for both fields.
func TestQRPreservesSpecialData(t *testing.T) {
	ctx := context.Background()
	q := newQR(t)

	cases := map[string]string{
		"empty":               "",
		"single space":        " ",
		"ascii":               "TKT-98421049-X7",
		"emoji":               "🎫🎉🔥 party 😀😂🥳",
		"emoji zwj sequence":  "👨‍👩‍👧‍👦 👩🏽‍💻 🏳️‍🌈",
		"emoji skin tones":    "👍🏻👍🏼👍🏽👍🏾👍🏿",
		"flags":               "🇵🇭🇺🇸🇯🇵",
		"cjk":                 "全球技术开发者大会 東京 서울",
		"filipino":            "Ñandú, Peña, Cañete, Niño — maligayang bati",
		"accents":             "café résumé naïve Zoë Ångström Łódź",
		"arabic rtl":          "مرحبا بالعالم",
		"hebrew rtl":          "שלום עולם",
		"devanagari":          "नमस्ते दुनिया",
		"thai":                "สวัสดีชาวโลก",
		"combining marks":     "e\u0301 a\u0308 n\u0303 (decomposed)",
		"math and currency":   "∑ ∫ √ ≠ ≤ ∞ € £ ¥ ₱ ₹ ©®™",
		"double quotes":       `He said "hello" and left`,
		"single quotes":       `it's a 'test'`,
		"backslashes":         `C:\Users\jerbee\file.txt \n \t \\`,
		"json lookalike":      `{"data":"x","type":"y"}`,
		"json escapes":        `\u0041 \" \/ \b \f`,
		"html chars":          `<script>alert("x & y")</script>`,
		"sql chars":           `'; DROP TABLE users; --`,
		"newlines and tabs":   "line1\nline2\r\nline3\ttabbed",
		"control chars":       "\x01\x02\x07\x08\x1b[31mred\x1b[0m\x7f",
		"null bytes":          "before\x00after\x00",
		"unicode separators":  "a\u2028b\u2029c\u0085d",
		"zero width and bom":  "\ufeffzero\u200bwidth\u200djoiner\u2060",
		"noncharacters":       "\uffff\ufffe\U0010ffff",
		"replacement char":    "\ufffd literal replacement char",
		"private use":         "\ue000\uf8ff\U000f0000",
		"url with query":      "https://x.io/a?b=c&d=%F0%9F%8E%AB#frag+plus",
		"base64 lookalike":    "SGVsbG8gV29ybGQ=+/==",
		"very long":           strings.Repeat("Ünï🎫çødé-", 20_000),
		"long unbroken emoji": strings.Repeat("😀", 50_000),
	}

	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			// Put the value in both fields so Data and Type are both covered.
			want := qr.QRData{Data: value, Type: value}

			enc, err := q.Encode(ctx, &want)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got, err := q.Decode(ctx, enc)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.Data != want.Data {
				t.Errorf("Data changed:\n got %q\nwant %q", trunc(got.Data), trunc(want.Data))
			}
			if got.Type != want.Type {
				t.Errorf("Type changed:\n got %q\nwant %q", trunc(got.Type), trunc(want.Type))
			}
		})
	}
}

// The encoded output goes into a QR code, so it must stay plain base64 text
// no matter what the input contains.
func TestQREncodedOutputIsPlainBase64(t *testing.T) {
	ctx := context.Background()
	q := newQR(t)

	for _, value := range []string{"🎫", "مرحبا", "a\x00b", `"quoted"`, "全球"} {
		enc, err := q.Encode(ctx, &qr.QRData{Data: value, Type: "ticket"})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range enc {
			isB64 := r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' ||
				r >= '0' && r <= '9' || r == '+' || r == '/' || r == '='
			if !isB64 {
				t.Fatalf("encoded output for %q has non-base64 char %q", value, r)
			}
		}
	}
}

// Invalid UTF-8 bytes (e.g. raw binary or a mis-decoded string) must come back
// unchanged. sonic does this; the stdlib encoding/json would silently replace
// them with U+FFFD, so this guards against swapping the JSON library.
func TestQRPreservesInvalidUTF8(t *testing.T) {
	ctx := context.Background()
	q := newQR(t)
	want := qr.QRData{Data: "ok\xff\xfe\xc3\x28end", Type: "ticket"}

	enc, err := q.Encode(ctx, &want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := q.Decode(ctx, enc)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("invalid UTF-8 changed:\n got %q\nwant %q", got.Data, want.Data)
	}
}

func trunc(s string) string {
	const max = 80
	if r := []rune(s); len(r) > max {
		return string(r[:max]) + "…"
	}
	return s
}

func TestQREncodeIsNonDeterministic(t *testing.T) {
	ctx := context.Background()
	qr := newQR(t)
	data := sampleQRData()

	a, _ := qr.Encode(ctx, &data)
	b, _ := qr.Encode(ctx, &data)
	if a == b {
		t.Fatal("two encodes produced identical output: nonce is being reused")
	}
}

func TestQRRejectsBadInput(t *testing.T) {
	ctx := context.Background()
	q := newQR(t)
	data := sampleQRData()
	enc, _ := q.Encode(ctx, &data)

	tampered := []byte(enc)
	tampered[len(tampered)/2] ^= 1

	other := qrService.NewQRService("a-different-secret")

	cases := map[string]struct {
		svc   qr.QRServices
		input string
	}{
		"tampered ciphertext": {q, string(tampered)},
		"wrong secret":        {other, enc},
		"not base64":          {q, "!!!not-base64!!!"},
		"too short":           {q, "YWJj"},
		"empty":               {q, ""},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := c.svc.Decode(ctx, c.input); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// Fails if per-call construction of the encoder/decoder/AEAD returns
// (that version allocated hundreds of KB and dozens of objects per call).
func TestQRAllocationBudget(t *testing.T) {
	ctx := context.Background()
	qr := newQR(t)
	data := sampleQRData()

	allocs := testing.AllocsPerRun(200, func() {
		enc, err := qr.Encode(ctx, &data)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := qr.Decode(ctx, enc); err != nil {
			t.Fatal(err)
		}
	})

	const maxAllocs = 40 // measured ~12
	if allocs > maxAllocs {
		t.Fatalf("round trip made %.0f allocs, budget is %d", allocs, maxAllocs)
	}
}

// 10,000 round trips took ~0.26s when fixed and ~40s with the old per-call setup.
func TestQRSpeedBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timing test in -short mode")
	}
	if raceEnabled {
		t.Skip("skipping timing test under -race (timings are ~14x slower)")
	}
	ctx := context.Background()
	qr := newQR(t)
	data := sampleQRData()

	const n = 10_000
	const budget = 3 * time.Second

	start := time.Now()
	for range n {
		enc, err := qr.Encode(ctx, &data)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := qr.Decode(ctx, enc); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed > budget {
		t.Fatalf("%d round trips took %v, budget is %v", n, elapsed, budget)
	}
}

// The service is shared across goroutines; run with -race to catch data races.
func TestQRConcurrentUse(t *testing.T) {
	ctx := context.Background()
	qr := newQR(t)
	data := sampleQRData()

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Go(func() {
			for range 200 {
				enc, err := qr.Encode(ctx, &data)
				if err != nil {
					errs <- err
					return
				}
				got, err := qr.Decode(ctx, enc)
				if err != nil {
					errs <- err
					return
				}
				if got != data {
					errs <- fmt.Errorf("concurrent round trip changed data: got %+v", got)
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
