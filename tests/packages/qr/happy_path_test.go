package qr_test

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
)

func TestHappyPath_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newStarted(t)
	cases := map[string]qr.QRData{
		"simple":      {Data: "member:42", Type: "member"},
		"json data":   {Data: `{"id":42,"amount":"1000.00"}`, Type: "payment"},
		"unicode":     {Data: "Ñoño 日本 🧾", Type: "receipt"},
		"url":         {Data: "https://ecoop.example/verify?id=42&sig=abc", Type: "link"},
		"quotes":      {Data: `he said "hi" \ back\slash`, Type: `"quoted"`},
		"large":       {Data: strings.Repeat("loan-ledger-line;", 2000), Type: "bulk"},
		"only data":   {Data: "x"},
		"only type":   {Type: "x"},
		"whitespace":  {Data: "  \t\n  ", Type: " "},
		"html-ish":    {Data: "<script>alert(1)</script>", Type: "&amp;"},
		"line breaks": {Data: "line1\r\nline2", Type: "multi"},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			enc := mustEncode(t, svc, &in)
			out, err := svc.Decode(ctx, enc)
			if err != nil || out != in {
				t.Fatalf("Decode = %+v, %v; want %+v", out, err, in)
			}
		})
	}
}

func TestHappyPath_OutputIsStdBase64(t *testing.T) {
	t.Parallel()
	enc := mustEncode(t, newStarted(t), &qr.QRData{Data: "x", Type: "y"})
	if _, err := base64.StdEncoding.DecodeString(enc); err != nil {
		t.Fatalf("output is not standard base64: %v", err)
	}
}

func TestHappyPath_OutputHidesContent(t *testing.T) {
	t.Parallel()
	enc := mustEncode(t, newStarted(t), &qr.QRData{Data: "SECRET-MARKER", Type: "TYPE-MARKER"})
	raw, _ := base64.StdEncoding.DecodeString(enc)
	for _, s := range []string{enc, string(raw)} {
		if strings.Contains(s, "SECRET-MARKER") || strings.Contains(s, "TYPE-MARKER") {
			t.Fatal("plaintext visible in encoded output")
		}
	}
}

func TestHappyPath_NonceMakesOutputUnique(t *testing.T) {
	t.Parallel()
	svc := newStarted(t)
	d := &qr.QRData{Data: "same", Type: "same"}
	if mustEncode(t, svc, d) == mustEncode(t, svc, d) {
		t.Fatal("identical outputs for identical input; nonce is not random")
	}
}

func TestHappyPath_CompressionShrinksRepetitiveData(t *testing.T) {
	t.Parallel()
	d := &qr.QRData{Data: strings.Repeat("A", 10_000), Type: "bulk"}
	enc := mustEncode(t, newStarted(t), d)
	if len(enc) >= len(d.Data) {
		t.Fatalf("encoded len %d not smaller than raw %d; compression not applied", len(enc), len(d.Data))
	}
}

func TestHappyPath_SameSecretInteroperates(t *testing.T) {
	t.Parallel()
	a, b := newStartedWithSecret(t, "shared"), newStartedWithSecret(t, "shared")
	in := qr.QRData{Data: "cross", Type: "node"}
	out, err := b.Decode(context.Background(), mustEncode(t, a, &in))
	if err != nil || out != in {
		t.Fatalf("instance B could not read A's QR: %+v, %v", out, err)
	}
}

func TestHappyPath_RestartKeepsWorking(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := qr.NewQRServices(testSecret, nil)
	for i := range 3 {
		if err := svc.Start(ctx); err != nil {
			t.Fatalf("cycle %d Start: %v", i, err)
		}
		in := qr.QRData{Data: "cycle", Type: "restart"}
		enc, err := svc.Encode(ctx, &in)
		if err != nil {
			t.Fatal(err)
		}
		if out, err := svc.Decode(ctx, enc); err != nil || out != in {
			t.Fatalf("cycle %d: %+v, %v", i, out, err)
		}
		if err := svc.Stop(ctx); err != nil {
			t.Fatalf("cycle %d Stop: %v", i, err)
		}
	}
}

func TestHappyPath_InputNotMutated(t *testing.T) {
	t.Parallel()
	in := &qr.QRData{Data: "keep", Type: "me"}
	mustEncode(t, newStarted(t), in)
	if in.Data != "keep" || in.Type != "me" {
		t.Fatalf("Encode mutated its input: %+v", in)
	}
}
