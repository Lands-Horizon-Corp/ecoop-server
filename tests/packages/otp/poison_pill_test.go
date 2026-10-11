package otp_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/otp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
)

// Poison pill: hostile keys, hostile guesses, and attempts to override or
// leak the code.

func TestPoisonPill_HostileKeys(t *testing.T) {
	o := newOTP(t)
	ctx := ctxT(t)
	for name, suffix := range map[string]string{
		"unicode":      "ключ-鍵-🔑",
		"newlines":     "a\r\nDEL otp:code:*",
		"null bytes":   "a\x00b",
		"glob":         "*?[]",
		"prefix-alike": "otp:attempts:x",
		"64KiB":        strings.Repeat("k", 64<<10),
	} {
		t.Run(name, func(t *testing.T) {
			k := key(t) + ":" + suffix
			var code string
			var err error
			mustNotPanic(t, "Generate", func() { code, err = o.Generate(ctx, k) })
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := o.Verify(ctx, k, code); !ok || err != nil {
				t.Fatalf("Verify = %v, %v", ok, err)
			}
			_ = o.Revoke(ctx, k)
		})
	}
}

func TestPoisonPill_HostileGuesses(t *testing.T) {
	o := newOTP(t)
	ctx := ctxT(t)
	for name, guess := range map[string]string{
		"empty":       "",
		"1MiB":        strings.Repeat("9", 1<<20),
		"sql":         "' OR 1=1 --",
		"glob":        "*",
		"null":        "\x00",
		"unicode":     "１２３４５６", // full-width digits
		"padded":      " 123456 ",
		"pepper form": secret + ":123456",
	} {
		t.Run(name, func(t *testing.T) {
			k := key(t)
			code, _ := o.Generate(ctx, k)
			if guess == code {
				t.Skip("guess collided with the code")
			}
			var ok bool
			var err error
			mustNotPanic(t, "Verify", func() { ok, err = o.Verify(ctx, k, guess) })
			if ok || err != nil {
				t.Fatalf("Verify(%.20q) = %v, %v; want false, nil", guess, ok, err)
			}
			if ok, err := o.Verify(ctx, k, code); !ok || err != nil {
				t.Fatalf("real code no longer works after a hostile guess: %v, %v", ok, err)
			}
		})
	}
}

func TestPoisonPill_CodeNotTrimmedOrNormalised(t *testing.T) {
	o := newOTP(t)
	ctx, k := ctxT(t), key(t)
	code, _ := o.Generate(ctx, k)
	for _, variant := range []string{" " + code, code + " ", code + "\n", code[:len(code)-1]} {
		if ok, _ := o.Verify(ctx, k, variant); ok {
			t.Fatalf("variant %q accepted", variant)
		}
	}
	if ok, _ := o.Verify(ctx, k, code); !ok {
		t.Fatal("exact code rejected")
	}
}

func TestPoisonPill_CallerCannotOverrideTheCode(t *testing.T) {
	o := newOTP(t)
	ctx, k := ctxT(t), key(t)
	// A caller (or user input reaching Vars) setting Code must not change
	// what is sent; the generated code always wins.
	code, err := o.GenerateSMTP(ctx, k, smtp.SMTPRequest{To: "a@ecoop.test", Body: "{{.Code}}", Vars: map[string]string{otp.CodeVar: "000000"}})
	if err != nil {
		t.Fatal(err)
	}
	if code == "000000" {
		t.Skip("generated code happened to be 000000")
	}
	if ok, _ := o.Verify(ctx, k, "000000"); ok {
		t.Fatal("caller-supplied Code was accepted")
	}
}

func TestPoisonPill_StoredValueIsNotTheCode(t *testing.T) {
	ch := newCache(t)
	o := startOTP(t, secret, ch, mockSMTP(t), mockSMS(t))
	ctx, k := ctxT(t), key(t)
	code, _ := o.Generate(ctx, k)
	stored, err := ch.Get(ctx, otp.KeyPrefix+k)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(stored), code) || !strings.HasPrefix(string(stored), "$argon2id$") {
		t.Fatalf("cache holds %q; want an argon2id hash that does not contain the code", stored)
	}
}

func TestPoisonPill_StolenHashUselessWithoutPepper(t *testing.T) {
	// The same code hashed by a service with another secret must not verify:
	// a leaked hash is useless without the server's pepper.
	ch := newCache(t)
	a := startOTP(t, secret, ch, mockSMTP(t), mockSMS(t))
	b := startOTP(t, "another-secret", ch, mockSMTP(t), mockSMS(t))
	ctx, k := ctxT(t), key(t)
	code, _ := a.Generate(ctx, k)
	if ok, err := b.Verify(ctx, k, code); ok || err != nil {
		t.Fatalf("a different pepper verified the code: %v, %v", ok, err)
	}
}

func TestPoisonPill_LockoutCannotBeResetByWrongKeys(t *testing.T) {
	o := newOTP(t)
	ctx, k, other := ctxT(t), key(t), key(t)
	code, _ := o.Generate(ctx, k)
	for range otp.MaxAttempts {
		_, _ = o.Verify(ctx, k, otherCode(code))
		_, _ = o.Verify(ctx, other, "123456") // noise on another key
	}
	if _, err := o.Verify(ctx, k, code); !errors.Is(err, otp.ErrTooManyAttempts) {
		t.Fatalf("lockout bypassed: %v", err)
	}
}
