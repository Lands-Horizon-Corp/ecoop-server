package qr_test

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
)

func TestSadPath_WrongSecret(t *testing.T) {
	t.Parallel()
	enc := mustEncode(t, newStartedWithSecret(t, "secret-a"), &qr.QRData{Data: "x"})
	out, err := newStartedWithSecret(t, "secret-b").Decode(context.Background(), enc)
	if err == nil {
		t.Fatalf("decoded with wrong secret: %+v", out)
	}
	if out != (qr.QRData{}) {
		t.Fatalf("non-zero QRData returned on failure: %+v", out)
	}
}

func TestSadPath_MalformedInput(t *testing.T) {
	t.Parallel()
	svc := newStarted(t)
	valid := mustEncode(t, svc, &qr.QRData{Data: "x", Type: "y"})
	raw, _ := base64.StdEncoding.DecodeString(valid)

	cases := map[string]string{
		"empty":              "",
		"not base64":         "this is not base64!!",
		"url base64":         base64.URLEncoding.EncodeToString(raw) + "-_",
		"raw base64":         strings.TrimRight(valid, "="),
		"shorter than nonce": base64.StdEncoding.EncodeToString(raw[:10]),
		"nonce only":         base64.StdEncoding.EncodeToString(raw[:24]),
		"truncated tag":      base64.StdEncoding.EncodeToString(raw[:len(raw)-1]),
		"extra byte":         base64.StdEncoding.EncodeToString(append(append([]byte(nil), raw...), 0)),
		"leading space":      " " + valid,
		"plain json":         `{"data":"x","type":"y"}`,
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := svc.Decode(context.Background(), in)
			if err == nil {
				t.Fatalf("accepted malformed input, got %+v", out)
			}
			if out != (qr.QRData{}) {
				t.Fatalf("non-zero QRData on failure: %+v", out)
			}
		})
	}
}

func TestSadPath_TamperedPayload(t *testing.T) {
	t.Parallel()
	svc := newStarted(t)
	raw, _ := base64.StdEncoding.DecodeString(mustEncode(t, svc, &qr.QRData{Data: "amount:100"}))
	for _, i := range []int{0, 23, 24, len(raw) / 2, len(raw) - 1} {
		mutated := append([]byte(nil), raw...)
		mutated[i] ^= 0x01
		if _, err := svc.Decode(context.Background(), base64.StdEncoding.EncodeToString(mutated)); err == nil {
			t.Errorf("tampered byte %d accepted", i)
		}
	}
}

func TestSadPath_AuthenticButNotZstd(t *testing.T) {
	t.Parallel()
	svc := newStarted(t)
	enc := forge(t, testSecret, []byte(`{"data":"x","type":"y"}`), false)
	if _, err := svc.Decode(context.Background(), enc); err == nil {
		t.Fatal("uncompressed plaintext accepted")
	}
}

func TestSadPath_AuthenticButNotJSON(t *testing.T) {
	t.Parallel()
	svc := newStarted(t)
	for name, body := range map[string]string{
		"text":        "hello",
		"wrong types": `{"data":42,"type":true}`,
		"array":       `[1,2,3]`,
		"truncated":   `{"data":"x"`,
		"empty":       ``,
	} {
		t.Run(name, func(t *testing.T) {
			if out, err := svc.Decode(context.Background(), forge(t, testSecret, []byte(body), true)); err == nil {
				t.Fatalf("accepted %q as %+v", body, out)
			}
		})
	}
}
