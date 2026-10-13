package security_test

import (
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
)

const (
	testSecret      = "ecoop-test-secret"
	testMemory      = 8 * 1024 // KiB, kept small so the suite stays fast
	testIterations  = 1
	testParallelism = 2
	testSalt        = 16
	testKey         = 32
)

var _ security.ISecurityService = (*security.SecurityService)(nil)

func newService(t testing.TB) security.ISecurityService {
	t.Helper()
	return newServiceWithSecret(t, testSecret)
}

func newServiceWithSecret(t testing.TB, secret string) security.ISecurityService {
	t.Helper()
	svc := security.NewSecurityService(secret, testMemory, testIterations, testParallelism, testSalt, testKey)
	if svc == nil {
		t.Fatal("NewSecurityService returned nil")
	}
	return svc
}

// capturePanic runs fn and returns the recovered panic, if any, so one
// panicking case cannot abort the whole test binary.
func capturePanic(fn func()) (recovered any, stack string) {
	defer func() {
		if r := recover(); r != nil {
			recovered, stack = r, string(debug.Stack())
		}
	}()
	fn()
	return nil, ""
}

func mustNotPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	if r, stack := capturePanic(fn); r != nil {
		if testing.Verbose() {
			t.Log(stack)
		}
		t.Fatalf("%s panicked: %v", name, r)
	}
}

func mustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	if r, _ := capturePanic(fn); r == nil {
		t.Fatalf("%s: expected panic, got none", name)
	}
}

func argonHash(algo string, version, m, tm, p int, salt, hash string) string {
	return fmt.Sprintf("$%s$v=%d$m=%d,t=%d,p=%d$%s$%s", algo, version, m, tm, p, salt, hash)
}
