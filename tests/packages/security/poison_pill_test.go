package security_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
)

// Poison pill: attacker-shaped inputs that try to crash, stall, or confuse the service.

func TestPoisonPill_HashParamsCannotExhaustResources(t *testing.T) {
	t.Parallel()
	svc := newService(t)
	cases := map[string]string{
		"4GiB memory":       argonHash("argon2id", 19, 1<<32-1, 1, 1, "c2FsdA", "aGFzaA"),
		"max iterations":    argonHash("argon2id", 19, 8192, 1<<32-1, 1, "c2FsdA", "aGFzaA"),
		"uint8 overflow p":  argonHash("argon2id", 19, 8192, 1, 256, "c2FsdA", "aGFzaA"),
		"uint32 overflow m": argonHash("argon2id", 19, 1<<33, 1, 1, "c2FsdA", "aGFzaA"),
		"negative memory":   "$argon2id$v=19$m=-1,t=1,p=1$c2FsdA$aGFzaA",
		"negative iters":    "$argon2id$v=19$m=8192,t=-1,p=1$c2FsdA$aGFzaA",
		"hex memory":        "$argon2id$v=19$m=0xffffffff,t=1,p=1$c2FsdA$aGFzaA",
	}
	for name, hash := range cases {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			var ok bool
			var err error
			mustNotPanic(t, "VerifyPassword", func() {
				ok, err = svc.VerifyPassword(context.Background(), hash, "pw")
			})
			if ok || err == nil {
				t.Errorf("VerifyPassword = %v, %v; want false, error", ok, err)
			}
			if d := time.Since(start); d > time.Second {
				t.Errorf("rejection took %v; hostile params were likely evaluated", d)
			}
		})
	}
}

func TestPoisonPill_AlgorithmAndVersionDowngrade(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	hash, _ := svc.GeneratePassword(ctx, "pw")
	parts := strings.Split(hash, "$")

	cases := map[string]string{
		"argon2i":    strings.Join([]string{"", "argon2i", parts[2], parts[3], parts[4], parts[5]}, "$"),
		"bcrypt tag": strings.Join([]string{"", "bcrypt", parts[2], parts[3], parts[4], parts[5]}, "$"),
		"empty tag":  strings.Join([]string{"", "", parts[2], parts[3], parts[4], parts[5]}, "$"),
		"v=16":       strings.Join([]string{"", parts[1], "v=16", parts[3], parts[4], parts[5]}, "$"),
		"no prefix":  strings.Join([]string{"x", parts[1], parts[2], parts[3], parts[4], parts[5]}, "$"),
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			if ok, _ := svc.VerifyPassword(ctx, h, "pw"); ok {
				t.Fatalf("hash with forged header %q verified as valid", h[:strings.Index(h[1:], "$m")+1])
			}
		})
	}
}

func TestPoisonPill_HostilePasswords(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)

	t.Run("null bytes are significant", func(t *testing.T) {
		hash, _ := svc.GeneratePassword(ctx, "pw\x00suffix")
		if ok, _ := svc.VerifyPassword(ctx, hash, "pw"); ok {
			t.Fatal("password truncated at NUL byte")
		}
		if ok, _ := svc.VerifyPassword(ctx, hash, "pw\x00suffix"); !ok {
			t.Fatal("password with NUL byte did not verify")
		}
	})

	t.Run("invalid utf8", func(t *testing.T) {
		pw := string([]byte{0xff, 0xfe, 0xc3, 0x28})
		hash, err := svc.GeneratePassword(ctx, pw)
		if err != nil {
			t.Fatal(err)
		}
		if ok, _ := svc.VerifyPassword(ctx, hash, pw); !ok {
			t.Fatal("invalid UTF-8 password did not verify")
		}
	})

	t.Run("1MiB password", func(t *testing.T) {
		pw := strings.Repeat("A", 1<<20)
		hash, err := svc.GeneratePassword(ctx, pw)
		if err != nil {
			t.Fatal(err)
		}
		if ok, _ := svc.VerifyPassword(ctx, hash, pw[:len(pw)-1]); ok {
			t.Fatal("long password matched after truncation")
		}
	})
}

func TestPoisonPill_HashSwapAcrossKeyLength(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	long := security.NewSecurityService(testSecret, testMemory, testIterations, testParallelism, testSalt, 64)
	short := newService(t)
	hash, _ := long.GeneratePassword(ctx, "pw")
	if ok, _ := short.VerifyPassword(ctx, hash, "pw"); ok {
		t.Fatal("hash with 64-byte key matched a 32-byte derivation")
	}
}

func TestPoisonPill_HostileTokens(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	valid, _ := svc.Encrypt(ctx, "x", time.Minute)
	body := strings.TrimPrefix(valid, "v4.local.")

	cases := map[string]string{
		"empty":            "",
		"prefix only":      "v4.local.",
		"v2 header":        "v2.local." + body,
		"v3 header":        "v3.local." + body,
		"public purpose":   "v4.public." + body,
		"uppercase header": "V4.LOCAL." + body,
		"padded body":      valid + "==",
		"whitespace":       " " + valid,
		"newline":          valid + "\n",
		"forged footer":    valid + "." + base64.RawURLEncoding.EncodeToString([]byte(`{"kid":"../../etc/passwd"}`)),
		"jwt":              "eyJhbGciOiJub25lIn0.eyJwYXlsb2FkIjoieCJ9.",
		"garbage body":     "v4.local." + strings.Repeat("A", 200),
		"huge":             "v4.local." + strings.Repeat("A", 4<<20),
		"null bytes":       "v4.local.\x00\x00\x00",
	}
	for name, tok := range cases {
		t.Run(name, func(t *testing.T) {
			var err error
			mustNotPanic(t, "Decrypt", func() { _, err = svc.Decrypt(ctx, tok) })
			if err == nil {
				t.Fatal("hostile token accepted")
			}
		})
	}
}

func TestPoisonPill_PayloadIsOpaque(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	for _, p := range []string{
		`","exp":"2099-01-01T00:00:00Z","payload":"admin`,
		`{"payload":"nested"}`,
		"\x00\x01\x02",
		"v4.local.inner",
	} {
		tok, err := svc.Encrypt(ctx, p, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		got, err := svc.Decrypt(ctx, tok)
		if err != nil || got != p {
			t.Fatalf("payload %q came back as %q (err %v)", p, got, err)
		}
	}
}

func TestPoisonPill_HostileCiphertext(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	cases := map[string][]byte{
		"all 0xff":  bytes.Repeat([]byte{0xff}, 64),
		"16MiB":     make([]byte, 16<<20),
		"one byte":  {0x00},
		"text":      []byte("not a ciphertext at all, just text"),
		"own token": func() []byte { s, _ := svc.Encrypt(ctx, "x", time.Minute); return []byte(s) }(),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			var err error
			mustNotPanic(t, "DecryptData", func() { _, err = svc.DecryptData(ctx, in) })
			if !errors.Is(err, security.ErrDecrypt) {
				t.Fatalf("err = %v, want ErrDecrypt", err)
			}
		})
	}
}

func TestPoisonPill_DecryptErrorDoesNotLeakCause(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)
	ct, _ := svc.EncryptData(ctx, []byte("x"))
	ct[0] ^= 0xff
	_, err := svc.DecryptData(ctx, ct)
	if err != security.ErrDecrypt {
		t.Fatalf("err = %#v; want the bare ErrDecrypt sentinel (no oracle detail)", err)
	}
}
