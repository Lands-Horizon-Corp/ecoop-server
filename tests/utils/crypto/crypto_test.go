package crypto_test

import (
	"regexp"
	"sync"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

func TestRandomTokenAndHex(t *testing.T) {
	t.Parallel()
	if tok := utils.RandomToken(32); !regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`).MatchString(tok) {
		t.Fatalf("RandomToken(32) = %q: want 43 base64url characters", tok)
	}
	if h := utils.RandomHex(12); !regexp.MustCompile(`^[0-9a-f]{24}$`).MatchString(h) {
		t.Fatalf("RandomHex(12) = %q", h)
	}
	var mu sync.Mutex
	seen := map[string]bool{}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 1000 {
				tok := utils.RandomToken(16)
				mu.Lock()
				if seen[tok] {
					t.Error("RandomToken repeated a value")
				}
				seen[tok] = true
				mu.Unlock()
			}
		})
	}
	wg.Wait()
}

func TestHashHex(t *testing.T) {
	t.Parallel()
	// SHA-256("abc"), from FIPS 180-2.
	if got := utils.HashHex("abc"); got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("HashHex(abc) = %s", got)
	}
	if utils.HashHex("a") == utils.HashHex("b") {
		t.Fatal("different inputs, same hash")
	}
}
