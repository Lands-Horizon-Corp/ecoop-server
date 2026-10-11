package security_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Sync & race: one shared service hammered from many goroutines.
// Run with `go test -race` to catch data races.

const workers = 32

func TestRace_SharedServiceAllOperations(t *testing.T) {
	ctx := context.Background()
	svc := newService(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var failures atomic.Int64

	for w := range workers {
		wg.Go(func() {
			<-start // release all workers together to maximise overlap
			for i := range 5 {
				id := fmt.Sprintf("w%d-i%d", w, i)

				hash, err := svc.GeneratePassword(ctx, id)
				if ok, verr := svc.VerifyPassword(ctx, hash, id); err != nil || verr != nil || !ok {
					failures.Add(1)
					t.Errorf("%s: password round trip failed: %v %v %v", id, err, verr, ok)
				}
				tok, err := svc.Encrypt(ctx, id, time.Minute)
				if got, derr := svc.Decrypt(ctx, tok); err != nil || derr != nil || got != id {
					failures.Add(1)
					t.Errorf("%s: token round trip got %q: %v %v", id, got, err, derr)
				}
				ct, err := svc.EncryptData(ctx, []byte(id))
				if pt, derr := svc.DecryptData(ctx, ct); err != nil || derr != nil || string(pt) != id {
					failures.Add(1)
					t.Errorf("%s: data round trip got %q: %v %v", id, pt, err, derr)
				}
			}
		})
	}
	close(start)
	wg.Wait()
	if n := failures.Load(); n > 0 {
		t.Fatalf("%d concurrent failures", n)
	}
}

func TestRace_SaltsUniqueUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	svc := newService(t)
	var mu sync.Mutex
	seen := make(map[string]int)
	var wg sync.WaitGroup

	for w := range workers {
		wg.Go(func() {
			for range 4 {
				hash, err := svc.GeneratePassword(ctx, "same-password")
				if err != nil {
					t.Error(err)
					return
				}
				salt := strings.Split(hash, "$")[4]
				mu.Lock()
				if prev, dup := seen[salt]; dup {
					t.Errorf("salt reused by workers %d and %d", prev, w)
				}
				seen[salt] = w
				mu.Unlock()
			}
		})
	}
	wg.Wait()
}

func TestRace_NoncesUniqueUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	svc := newService(t)
	var mu sync.Mutex
	seen := make(map[string]struct{})
	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			for range 200 {
				ct, err := svc.EncryptData(ctx, []byte("same"))
				if err != nil {
					t.Error(err)
					return
				}
				nonce := string(ct[:12])
				mu.Lock()
				if _, dup := seen[nonce]; dup {
					t.Error("GCM nonce reused under concurrency")
				}
				seen[nonce] = struct{}{}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
}

func TestRace_SharedCiphertextReadConcurrently(t *testing.T) {
	ctx := context.Background()
	svc := newService(t)
	ct, _ := svc.EncryptData(ctx, []byte("shared"))
	tok, _ := svc.Encrypt(ctx, "shared", time.Minute)
	hash, _ := svc.GeneratePassword(ctx, "shared")

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			// The same input slice/string is read by every goroutine; none may mutate it.
			if pt, err := svc.DecryptData(ctx, ct); err != nil || string(pt) != "shared" {
				t.Errorf("DecryptData: %q %v", pt, err)
			}
			if got, err := svc.Decrypt(ctx, tok); err != nil || got != "shared" {
				t.Errorf("Decrypt: %q %v", got, err)
			}
			if ok, err := svc.VerifyPassword(ctx, hash, "shared"); err != nil || !ok {
				t.Errorf("VerifyPassword: %v %v", ok, err)
			}
		})
	}
	wg.Wait()
}

func TestRace_ManyServicesConstructedConcurrently(t *testing.T) {
	ctx := context.Background()
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			secret := fmt.Sprintf("tenant-%d", w%4)
			a, b := newServiceWithSecret(t, secret), newServiceWithSecret(t, secret)
			ct, _ := a.EncryptData(ctx, []byte(secret))
			if pt, err := b.DecryptData(ctx, ct); err != nil || string(pt) != secret {
				t.Errorf("tenant %s: %q %v", secret, pt, err)
			}
		})
	}
	wg.Wait()
}
