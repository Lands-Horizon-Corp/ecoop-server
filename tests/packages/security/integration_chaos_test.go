package security_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
)

// Integration chaos: the operations chained the way callers combine them,
// with faults injected at random stages and across multiple service instances.

type stage int

const (
	stageNone stage = iota
	stageHash
	stageSealed
	stageToken
	stageWrongSvc
)

// pipeline: hash password -> seal hash bytes -> wrap in token -> unwrap -> open -> verify.
func runPipeline(ctx context.Context, issuer, reader security.ISecurityService, pw string, fault stage, r *rand.Rand) error {
	hash, err := issuer.GeneratePassword(ctx, pw)
	if err != nil {
		return fmt.Errorf("hash: %w", err)
	}
	if fault == stageHash {
		last := "A"
		if hash[len(hash)-1] == 'A' {
			last = "B"
		}
		hash = hash[:len(hash)-1] + last
	}

	sealed, err := issuer.EncryptData(ctx, []byte(hash))
	if err != nil {
		return fmt.Errorf("seal: %w", err)
	}
	if fault == stageSealed {
		sealed[r.IntN(len(sealed))] ^= byte(1 + r.IntN(255))
	}

	tok, err := issuer.Encrypt(ctx, base64.StdEncoding.EncodeToString(sealed), time.Minute)
	if err != nil {
		return fmt.Errorf("token: %w", err)
	}
	if fault == stageToken {
		b := []byte(tok)
		i := len("v4.local.") + r.IntN(len(b)-len("v4.local."))
		if b[i] == 'x' {
			b[i] = 'y'
		} else {
			b[i] = 'x'
		}
		tok = string(b)
	}

	payload, err := reader.Decrypt(ctx, tok)
	if err != nil {
		return fmt.Errorf("untoken: %w", err)
	}
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	opened, err := reader.DecryptData(ctx, raw)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	ok, err := reader.VerifyPassword(ctx, string(opened), pw)
	if err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	if !ok {
		return errors.New("verify: mismatch")
	}
	return nil
}

func TestIntegrationChaos_PipelineWithRandomFaults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	primary := newServiceWithSecret(t, "primary")
	replica := newServiceWithSecret(t, "primary")
	rogue := newServiceWithSecret(t, "rogue")
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)

	const runs = 64
	var wg sync.WaitGroup
	errs := make(chan error, runs)
	for i := range runs {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(seed, uint64(i)))
			fault := stage(r.IntN(5))
			reader := replica
			if fault == stageWrongSvc {
				reader = rogue
			}
			err := runPipeline(ctx, primary, reader, fmt.Sprintf("pw-%d", i), fault, r)
			switch {
			case fault == stageNone && err != nil:
				errs <- fmt.Errorf("run %d: clean pipeline failed: %w", i, err)
			case fault != stageNone && err == nil:
				errs <- fmt.Errorf("run %d: fault %d went undetected", i, fault)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestIntegrationChaos_SecretRotation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	oldSvc := newServiceWithSecret(t, "secret-v1")
	newSvc := newServiceWithSecret(t, "secret-v2")

	hash, _ := oldSvc.GeneratePassword(ctx, "pw")
	tok, _ := oldSvc.Encrypt(ctx, "session", time.Minute)
	ct, _ := oldSvc.EncryptData(ctx, []byte("pii"))

	// Password hashes are secret-independent and must survive rotation.
	if ok, err := newSvc.VerifyPassword(ctx, hash, "pw"); err != nil || !ok {
		t.Errorf("password hash broke across secret rotation: %v, %v", ok, err)
	}
	// Tokens and sealed data are bound to the secret and must not.
	if _, err := newSvc.Decrypt(ctx, tok); err == nil {
		t.Error("old token accepted after rotation")
	}
	if _, err := newSvc.DecryptData(ctx, ct); !errors.Is(err, security.ErrDecrypt) {
		t.Errorf("old ciphertext: err = %v, want ErrDecrypt", err)
	}
}

func TestIntegrationChaos_ParamUpgradeKeepsOldHashesValid(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	weak := security.NewSecurityService(testSecret, 4*1024, 1, 1, 16, testKey)
	strong := security.NewSecurityService(testSecret, 16*1024, 3, 4, 32, testKey)

	oldHash, _ := weak.GeneratePassword(ctx, "pw")
	if ok, err := strong.VerifyPassword(ctx, oldHash, "pw"); err != nil || !ok {
		t.Fatalf("hash from weaker params rejected after upgrade: %v, %v", ok, err)
	}
	newHash, _ := strong.GeneratePassword(ctx, "pw")
	if ok, err := weak.VerifyPassword(ctx, newHash, "pw"); err != nil || !ok {
		t.Fatalf("hash from stronger params rejected by older node: %v, %v", ok, err)
	}
}

func TestIntegrationChaos_TokenAndDataKeysAreSeparated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)

	ct, _ := svc.EncryptData(ctx, []byte("x"))
	if _, err := svc.Decrypt(ctx, "v4.local."+base64.RawURLEncoding.EncodeToString(ct)); err == nil {
		t.Error("EncryptData output accepted as a token")
	}
	tok, _ := svc.Encrypt(ctx, "x", time.Minute)
	if _, err := svc.DecryptData(ctx, []byte(tok)); !errors.Is(err, security.ErrDecrypt) {
		t.Errorf("token accepted as sealed data: %v", err)
	}
}

func TestIntegrationChaos_MixedExpiryUnderLoad(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newService(t)

	type issued struct {
		tok   string
		alive bool
	}
	var toks []issued
	for i := range 40 {
		ttl, alive := time.Hour, true
		if i%2 == 0 {
			ttl, alive = time.Second, false
		}
		tok, _ := svc.Encrypt(ctx, fmt.Sprint(i), ttl)
		toks = append(toks, issued{tok, alive})
	}
	time.Sleep(2100 * time.Millisecond)

	var wg sync.WaitGroup
	for i, it := range toks {
		wg.Go(func() {
			got, err := svc.Decrypt(ctx, it.tok)
			if it.alive && (err != nil || got != fmt.Sprint(i)) {
				t.Errorf("token %d should be alive: %q, %v", i, got, err)
			}
			if !it.alive && err == nil {
				t.Errorf("token %d should have expired", i)
			}
		})
	}
	wg.Wait()
}
