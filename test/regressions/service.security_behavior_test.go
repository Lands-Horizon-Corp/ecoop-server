package regressions

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/security"
)

func newSecurityWith(secret string, memory, iterations uint32, parallelism uint8, salt, key uint32) security.SecurityServices {
	return security.NewSecurityService(secret, memory, iterations, parallelism, salt, key)
}

func TestSecurityService_HashFormat(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	hash, err := sec.GeneratePassword(ctx, "pw")
	if err != nil {
		t.Fatalf("GeneratePassword: %v", err)
	}

	parts := strings.Split(hash, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		t.Fatalf("hash %q is not PHC argon2id format", hash)
	}
	if parts[2] != "v=19" {
		t.Errorf("version = %q; want v=19", parts[2])
	}
	if parts[3] != "m=8192,t=1,p=1" {
		t.Errorf("params = %q; want m=8192,t=1,p=1 (the service's configuration)", parts[3])
	}
	// RawStdEncoding: 16 salt bytes -> 22 chars, 32 hash bytes -> 43 chars, no padding.
	if len(parts[4]) != 22 || len(parts[5]) != 43 {
		t.Errorf("salt/hash lengths = %d/%d; want 22/43", len(parts[4]), len(parts[5]))
	}
	if strings.Contains(hash, "=") && strings.Contains(parts[4]+parts[5], "=") {
		t.Error("salt or hash is padded; want raw base64")
	}
}

func TestSecurityService_HashesAreSalted(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	seen := make(map[string]struct{})
	for range 20 {
		h, err := sec.GeneratePassword(ctx, "same-password")
		if err != nil {
			t.Fatalf("GeneratePassword: %v", err)
		}
		if _, dup := seen[h]; dup {
			t.Fatalf("hash %q repeated for the same password; salt is not random", h)
		}
		seen[h] = struct{}{}
	}
}

func TestSecurityService_VerifyPassword(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	cases := map[string]string{
		"empty":         "",
		"ascii":         "correct horse battery staple",
		"unicode":       "pässwörd-密码-🔐",
		"whitespace":    "  leading and trailing  ",
		"nul byte":      "ab\x00cd",
		"long":          strings.Repeat("a", 10_000),
		"dollar signs":  "$argon2id$v=19$m=1,t=1,p=1$x$y",
		"newline":       "line1\nline2",
		"case sensitve": "PassWord",
	}
	for name, pw := range cases {
		t.Run(name, func(t *testing.T) {
			hash, err := sec.GeneratePassword(ctx, pw)
			if err != nil {
				t.Fatalf("GeneratePassword: %v", err)
			}
			if ok, err := sec.VerifyPassword(ctx, hash, pw); err != nil || !ok {
				t.Fatalf("Verify(correct) = %v, %v; want true, nil", ok, err)
			}
			for _, wrong := range []string{pw + "x", "x" + pw, strings.ToUpper(pw) + "!"} {
				if ok, err := sec.VerifyPassword(ctx, hash, wrong); err != nil || ok {
					t.Fatalf("Verify(%q) = %v, %v; want false, nil", wrong, ok, err)
				}
			}
		})
	}

	t.Run("password is not accepted for another user's hash", func(t *testing.T) {
		h1, _ := sec.GeneratePassword(ctx, "alice-pw")
		h2, _ := sec.GeneratePassword(ctx, "bob-pw")
		if ok, _ := sec.VerifyPassword(ctx, h1, "bob-pw"); ok {
			t.Error("bob's password verified against alice's hash")
		}
		if ok, _ := sec.VerifyPassword(ctx, h2, "alice-pw"); ok {
			t.Error("alice's password verified against bob's hash")
		}
	})

	t.Run("hash stays valid after the service parameters change", func(t *testing.T) {
		old := newSecurityWith("s", 8*1024, 1, 1, 16, 32)
		upgraded := newSecurityWith("s", 16*1024, 2, 2, 16, 32)
		hash, err := old.GeneratePassword(ctx, "pw")
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := upgraded.VerifyPassword(ctx, hash, "pw"); err != nil || !ok {
			t.Fatalf("old hash on upgraded service = %v, %v; want true, nil (params come from the hash)", ok, err)
		}
	})

	t.Run("hash does not depend on the token secret", func(t *testing.T) {
		a := newSecurityWith("secret-a", 8*1024, 1, 1, 16, 32)
		b := newSecurityWith("secret-b", 8*1024, 1, 1, 16, 32)
		hash, _ := a.GeneratePassword(ctx, "pw")
		if ok, err := b.VerifyPassword(ctx, hash, "pw"); err != nil || !ok {
			t.Fatalf("Verify across secrets = %v, %v; want true, nil", ok, err)
		}
	})

	t.Run("different output key length does not verify", func(t *testing.T) {
		k32 := newSecurityWith("s", 8*1024, 1, 1, 16, 32)
		k16 := newSecurityWith("s", 8*1024, 1, 1, 16, 16)
		hash, _ := k32.GeneratePassword(ctx, "pw")
		if ok, _ := k16.VerifyPassword(ctx, hash, "pw"); ok {
			t.Error("hash verified despite a different key length")
		}
	})
}

func TestSecurityService_VerifyPasswordRejectsMalformedHash(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	valid, _ := sec.GeneratePassword(ctx, "pw")
	parts := strings.Split(valid, "$")
	join := func(p []string) string { return strings.Join(p, "$") }
	with := func(i int, v string) string {
		p := append([]string(nil), parts...)
		p[i] = v
		return join(p)
	}

	cases := map[string]string{
		"empty":                "",
		"no separators":        "plain",
		"too few parts":        join(parts[:5]),
		"too many parts":       valid + "$extra",
		"params not numbers":   with(3, "m=a,t=b,p=c"),
		"params missing":       with(3, "m=8192"),
		"salt not base64":      with(4, "!!!!"),
		"hash not base64":      with(5, "!!!!"),
		"padded salt":          with(4, parts[4]+"=="),
		"params overflow":      with(3, "m=99999999999,t=1,p=1"),
		"parallelism overflow": with(3, "m=8192,t=1,p=999"),
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			ok, err := sec.VerifyPassword(ctx, h, "pw")
			if ok || err == nil {
				t.Fatalf("Verify(%q) = %v, %v; want false and an error", h, ok, err)
			}
		})
	}
}

// A stored hash is attacker-influenced input in some flows; it must not crash the process.
func TestSecurityService_VerifyPasswordDoesNotPanicOnZeroParams(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	valid, _ := sec.GeneratePassword(ctx, "pw")
	parts := strings.Split(valid, "$")

	for name, params := range map[string]string{
		"zero iterations":  "m=8192,t=0,p=1",
		"zero parallelism": "m=8192,t=1,p=0",
		"all zero":         "m=0,t=0,p=0",
	} {
		t.Run(name, func(t *testing.T) {
			p := append([]string(nil), parts...)
			p[3] = params
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("VerifyPassword panicked: %v", r)
				}
			}()
			if ok, err := sec.VerifyPassword(ctx, strings.Join(p, "$"), "pw"); ok || err == nil {
				t.Fatalf("Verify = %v, %v; want false and an error", ok, err)
			}
		})
	}
}

// The hash carries its own cost parameters, so a crafted one must not be able to force huge work.
func TestSecurityService_VerifyPasswordCapsHashCost(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	valid, _ := sec.GeneratePassword(ctx, "pw")
	parts := strings.Split(valid, "$")

	for name, params := range map[string]string{
		"memory 4 TiB":      "m=4294967295,t=1,p=1",
		"memory over cap":   "m=262145,t=1,p=1",
		"iterations 4 bil":  "m=8192,t=4294967295,p=1",
		"iterations at 17":  "m=8192,t=17,p=1",
		"parallelism at 65": "m=8192,t=1,p=65",
		"everything max":    "m=4294967295,t=4294967295,p=255",
	} {
		t.Run(name, func(t *testing.T) {
			p := append([]string(nil), parts...)
			p[3] = params

			start := time.Now()
			ok, err := sec.VerifyPassword(ctx, strings.Join(p, "$"), "pw")
			if ok || err == nil {
				t.Fatalf("Verify = %v, %v; want false and an error", ok, err)
			}
			if d := time.Since(start); d > time.Second {
				t.Fatalf("rejection took %v; the cap must apply before any hashing", d)
			}
		})
	}

	t.Run("production-sized params still verify", func(t *testing.T) {
		prod := newSecurityWith("s", 64*1024, 3, 4, 16, 32)
		hash, err := prod.GeneratePassword(ctx, "pw")
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := sec.VerifyPassword(ctx, hash, "pw"); err != nil || !ok {
			t.Fatalf("Verify = %v, %v; want true, nil", ok, err)
		}
	})
}

func TestSecurityService_EncryptTokenProperties(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	token, err := sec.Encrypt(ctx, "payload", time.Minute)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if !strings.HasPrefix(token, "v4.local.") {
		t.Errorf("token %q is not a PASETO v4.local token", token)
	}
	if strings.Contains(token, "payload") {
		t.Error("plaintext appears in the token; payload is not encrypted")
	}

	seen := map[string]struct{}{token: {}}
	for range 20 {
		next, err := sec.Encrypt(ctx, "payload", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, dup := seen[next]; dup {
			t.Fatal("two encryptions of the same payload gave the same token; nonce is not random")
		}
		seen[next] = struct{}{}
	}
}

func TestSecurityService_EncryptDecryptPayloads(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	cases := map[string]string{
		"empty":   "",
		"unicode": "名前 — José 🚀",
		"json":    `{"id":"u-1","roles":["admin","teller"]}`,
		"newline": "a\nb\r\nc",
		"nul":     "a\x00b",
		"large":   strings.Repeat("x", 1<<20),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			token, err := sec.Encrypt(ctx, payload, time.Minute)
			if err != nil {
				t.Fatalf("Encrypt: %v", err)
			}
			got, err := sec.Decrypt(ctx, token)
			if err != nil {
				t.Fatalf("Decrypt: %v", err)
			}
			if got != payload {
				t.Fatalf("roundtrip mismatch: got %d bytes, want %d", len(got), len(payload))
			}
		})
	}
}

func TestSecurityService_DecryptRejectsInvalidTokens(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	token, _ := sec.Encrypt(ctx, "data", time.Minute)

	flip := func(s string, i int) string {
		b := []byte(s)
		if b[i] == 'A' {
			b[i] = 'B'
		} else {
			b[i] = 'A'
		}
		return string(b)
	}

	cases := map[string]string{
		"empty":             "",
		"garbage":           "not-a-token",
		"wrong version":     strings.Replace(token, "v4.", "v3.", 1),
		"public purpose":    strings.Replace(token, ".local.", ".public.", 1),
		"header only":       "v4.local.",
		"truncated":         token[:len(token)/2],
		"extra suffix":      token + "AAAA",
		"flipped first":     flip(token, len("v4.local.")),
		"flipped middle":    flip(token, len(token)/2),
		"flipped last":      flip(token, len(token)-1),
		"leading space":     " " + token,
		"trailing newline":  token + "\n",
		"footer added":      token + ".Zm9vdGVy",
		"another service's": "",
	}
	other, _ := newSecurityWith("a-different-secret-key-value!!", 8*1024, 1, 1, 16, 32).Encrypt(ctx, "data", time.Minute)
	cases["another service's"] = other

	for name, tok := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := sec.Decrypt(ctx, tok)
			if err == nil {
				t.Fatalf("Decrypt succeeded with %q; want error", got)
			}
			if got != "" {
				t.Errorf("Decrypt returned %q alongside an error; want empty", got)
			}
		})
	}
}

func TestSecurityService_TokensAreBoundToTheSecret(t *testing.T) {
	ctx := context.Background()
	a := newSecurityWith("secret-a", 8*1024, 1, 1, 16, 32)
	sameA := newSecurityWith("secret-a", 1024, 5, 3, 8, 64)
	b := newSecurityWith("secret-b", 8*1024, 1, 1, 16, 32)

	token, err := a.Encrypt(ctx, "data", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := sameA.Decrypt(ctx, token); err != nil || got != "data" {
		t.Fatalf("same secret, other hashing params: Decrypt = %q, %v; want data, nil", got, err)
	}
	if _, err := b.Decrypt(ctx, token); err == nil {
		t.Fatal("token decrypted under a different secret")
	}
}

func TestSecurityService_TokenExpiry(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()

	t.Run("short ttl expires", func(t *testing.T) {
		token, _ := sec.Encrypt(ctx, "data", 1500*time.Millisecond)
		if _, err := sec.Decrypt(ctx, token); err != nil {
			t.Fatalf("Decrypt before expiry: %v", err)
		}
		time.Sleep(2 * time.Second)
		if _, err := sec.Decrypt(ctx, token); err == nil {
			t.Fatal("Decrypt succeeded after the ttl elapsed")
		}
	})

	t.Run("negative ttl is already expired", func(t *testing.T) {
		token, _ := sec.Encrypt(ctx, "data", -time.Hour)
		if _, err := sec.Decrypt(ctx, token); err == nil {
			t.Fatal("token with a negative ttl decrypted")
		}
	})

	t.Run("long ttl is accepted", func(t *testing.T) {
		token, _ := sec.Encrypt(ctx, "data", 365*24*time.Hour)
		if got, err := sec.Decrypt(ctx, token); err != nil || got != "data" {
			t.Fatalf("Decrypt = %q, %v; want data, nil", got, err)
		}
	})
}

func TestSecurityService_ConcurrentUse(t *testing.T) {
	sec := newTestSecurityService()
	ctx := context.Background()
	hash, err := sec.GeneratePassword(ctx, "shared")
	if err != nil {
		t.Fatal(err)
	}

	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers*3)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := fmt.Sprintf("payload-%d", i)
			token, err := sec.Encrypt(ctx, payload, time.Minute)
			if err != nil {
				errs <- err
				return
			}
			if got, err := sec.Decrypt(ctx, token); err != nil || got != payload {
				errs <- fmt.Errorf("worker %d: Decrypt = %q, %v", i, got, err)
			}
			if ok, err := sec.VerifyPassword(ctx, hash, "shared"); err != nil || !ok {
				errs <- fmt.Errorf("worker %d: Verify = %v, %v", i, ok, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
