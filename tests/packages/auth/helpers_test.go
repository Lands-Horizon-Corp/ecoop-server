//go:build integration

package auth_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
)

// Tests run against the valkey service in docker-compose.yml, configured
// from CACHE_* in the environment or the repo's .env. Each test uses its
// own app name, so its keys are its own; they are deleted when it ends.
var (
	valkeyAddr, valkeyPassword, skipReason string
	seq                                    atomic.Int64
)

const header = "X-CSRF-Token"

// user is the claim the tests sign in with.
type user struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Role  string `json:"role"`
}

func (u user) GetID() string { return u.ID }

var _ auth.AUTHServices[user] = (*auth.AuthService[user])(nil)

func TestMain(m *testing.M) {
	if err := startValkey(); err != nil {
		skipReason = "no Valkey: " + err.Error()
		fmt.Fprintln(os.Stderr, "auth tests:", skipReason)
	}
	os.Exit(m.Run())
}

func startValkey() error {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		if parent := filepath.Dir(dir); parent != dir {
			dir = parent
		} else {
			return errors.New("go.mod not found")
		}
	}
	env := map[string]string{}
	if data, err := os.ReadFile(filepath.Join(dir, ".env")); err == nil {
		for line := range strings.SplitSeq(string(data), "\n") {
			if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok && !strings.HasPrefix(k, "#") {
				env[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
			}
		}
	}
	for _, k := range []string{"CACHE_HOST", "CACHE_PORT", "CACHE_PASSWORD"} {
		if v, ok := os.LookupEnv(k); ok {
			env[k] = v
		}
		if env[k] == "" {
			return fmt.Errorf("set %s in .env or the environment", k)
		}
	}
	valkeyAddr, valkeyPassword = net.JoinHostPort(env["CACHE_HOST"], env["CACHE_PORT"]), env["CACHE_PASSWORD"]
	up := exec.Command("docker", "compose", "up", "-d", "--wait", "valkey")
	up.Dir = dir
	if out, err := up.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up valkey: %w\n%s", err, out)
	}
	return nil
}

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newCache is a running cache, stopped when t ends.
func newCache(t testing.TB) cache.CacheServices {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	c := cache.NewCacheService([]string{valkeyAddr}, valkeyPassword, nil)
	if err := c.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

func newSecurity() security.SecurityServices {
	return security.NewSecurityService("auth-test-secret", 8*1024, 1, 2, 16, 32)
}

// newAuth is an auth service under an app name unique to t, on c (a new
// cache if none); every key under that name is deleted when t ends.
func newAuth(t testing.TB, c ...cache.CacheServices) (auth.AUTHServices[user], cache.CacheServices, string) {
	t.Helper()
	ch := newCache(t)
	if len(c) > 0 {
		ch = c[0]
	}
	name := fmt.Sprintf("authtest%d%d", time.Now().UnixNano(), seq.Add(1))
	t.Cleanup(func() {
		keys, _ := ch.Keys(context.Background(), name+":*")
		for _, k := range keys {
			_, _ = ch.Delete(context.Background(), k)
		}
	})
	return auth.NewAuthService[user](name, header, false, ch, newSecurity()), ch, name
}

// request is a fresh request from origin.
func request(origin string) *app.RequestContext {
	c := app.NewContext(0)
	c.Request.Header.Set("Origin", origin)
	return c
}

// login signs u in from origin and returns the session token.
func login(t testing.TB, a auth.AUTHServices[user], u user, origin string) string {
	t.Helper()
	c := request(origin)
	if err := a.SetCSRF(ctxT(t), c, u, time.Hour); err != nil {
		t.Fatalf("SetCSRF: %v", err)
	}
	token := string(c.Response.Header.Peek(header))
	if token == "" {
		t.Fatal("no token in the response header")
	}
	return token
}

// with is a request from origin carrying token in the header.
func with(token, origin string) *app.RequestContext {
	c := request(origin)
	c.Request.Header.Set(header, token)
	return c
}

var ana = user{ID: "u-1", Email: "ana@ecoop.test", Role: "teller"}

const site = "https://app.ecoop.test"
