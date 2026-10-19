//go:build integration

package cache_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	valkey "github.com/valkey-io/valkey-go"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
)

// Server-backed tests run against the valkey service in docker-compose.yml,
// using CACHE_HOST, CACHE_PORT and CACHE_PASSWORD from the environment or the
// repo's .env. TestMain starts the service if needed. Nothing is flushed:
// every test deletes the keys it created when it finishes (see key).
var (
	testAddr     string
	testPassword string
	skipReason   string
	cleaner      cache.ICacheService // deletes each test's keys on cleanup
)

var _ cache.ICacheService = (*cache.CacheService)(nil)

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	if err := startValkey(); err != nil {
		skipReason = "no Valkey: " + err.Error()
		fmt.Fprintln(os.Stderr, "cache tests:", skipReason)
		return m.Run()
	}
	defer cleaner.Stop(context.Background())
	return m.Run()
}

func startValkey() error {
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	env, err := loadEnv(filepath.Join(root, ".env"))
	if err != nil {
		return err
	}
	var missing []string
	get := func(k string) string {
		v := env[k]
		if v == "" {
			missing = append(missing, k)
		}
		return v
	}
	host, port, password := get("CACHE_HOST"), get("CACHE_PORT"), get("CACHE_PASSWORD")
	if len(missing) > 0 {
		return fmt.Errorf("set %s in .env or the environment", strings.Join(missing, ", "))
	}
	testAddr, testPassword = net.JoinHostPort(host, port), password

	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker not installed")
	}
	up := exec.Command("docker", "compose", "up", "-d", "--wait", "valkey")
	up.Dir = root
	if out, err := up.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up valkey: %w\n%s", err, out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cleaner = cache.NewCacheService([]string{testAddr}, testPassword, nil)
	if err := cleaner.Run(ctx); err != nil {
		return fmt.Errorf("connecting to %s: %w", testAddr, err)
	}
	return nil
}

// loadEnv reads KEY=VALUE lines from path (if it exists); variables already
// set in the environment take precedence.
func loadEnv(path string) (map[string]string, error) {
	env := map[string]string{}
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(strings.TrimPrefix(line, "export "), "=")
		if !ok {
			continue
		}
		env[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
	}
	for _, k := range []string{"CACHE_HOST", "CACHE_PORT", "CACHE_PASSWORD"} {
		if v, ok := os.LookupEnv(k); ok {
			env[k] = v
		}
	}
	return env, nil
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found")
		}
		dir = parent
	}
}

func requireServer(t testing.TB) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

// requireFlush guards tests that wipe the whole database. They share the
// dev Valkey, so they only run when CACHE_TEST_ALLOW_FLUSH=1.
func requireFlush(t testing.TB) {
	t.Helper()
	requireServer(t)
	if os.Getenv("CACHE_TEST_ALLOW_FLUSH") != "1" {
		t.Skip("wipes the whole database; set CACHE_TEST_ALLOW_FLUSH=1 to run")
	}
}

func newRunning(t testing.TB) cache.ICacheService {
	t.Helper()
	return newRunningAt(t, testAddr)
}

func newRunningAt(t testing.TB, addr string) cache.ICacheService {
	t.Helper()
	requireServer(t)
	svc := cache.NewCacheService([]string{addr}, testPassword, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc
}

var (
	keyPrefixes sync.Map // testing.TB -> prefix
	keySeq      atomic.Int64
)

// key namespaces every key by test, and by run, so tests never see each
// other's data and `go test -count=N` never sees an earlier pass's keys.
// The first call for a test registers a cleanup that deletes every key under
// its prefix, so tests leave nothing behind in the shared Valkey.
func key(t testing.TB, parts ...string) string {
	p, loaded := keyPrefixes.LoadOrStore(t, fmt.Sprintf("test:%s#%d", t.Name(), keySeq.Add(1)))
	prefix := p.(string)
	if !loaded {
		t.Cleanup(func() { deletePattern(t, globEscape(prefix)+":*") })
	}
	return prefix + ":" + strings.Join(parts, ":")
}

// cleanupKey deletes k when the test ends, for keys made outside key().
func cleanupKey(t testing.TB, k string) {
	t.Cleanup(func() { deletePattern(t, globEscape(k)) })
}

func deletePattern(t testing.TB, pattern string) {
	if cleaner == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keys, err := cleaner.Keys(ctx, pattern)
	if err != nil {
		t.Logf("cleanup: listing %q: %v", pattern, err)
		return
	}
	cl := cleaner.Client()
	for len(keys) > 0 {
		n := min(len(keys), 500)
		if err := cl.Do(ctx, cl.B().Del().Key(keys[:n]...).Build()).Error(); err != nil {
			t.Logf("cleanup: deleting keys: %v", err)
			return
		}
		keys = keys[n:]
	}
}

// globEscape makes s match literally in a SCAN MATCH pattern.
func globEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune(`*?[]\`, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func pttl(t testing.TB, svc cache.ICacheService, k string) int64 {
	t.Helper()
	cl := svc.Client()
	n, err := cl.Do(context.Background(), cl.B().Pttl().Key(k).Build()).AsInt64()
	if err != nil {
		t.Fatalf("PTTL %s: %v", k, err)
	}
	return n
}

// isShutdownErr reports errors that are legitimate while a client is stopping.
func isShutdownErr(err error) bool {
	return errors.Is(err, cache.ErrNotRunning) || errors.Is(err, valkey.ErrClosing)
}

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

// eventually retries fn until it returns nil or the timeout passes.
func eventually(t *testing.T, timeout time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var err error
	for time.Now().Before(deadline) {
		if err = fn(); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %v", timeout, err)
}

// proxy is a TCP relay between the client and the server that can cut every
// connection or refuse new ones, to simulate network failures.
type proxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	down  bool
	delay atomic.Int64 // added to every server->client write, in ns
}

func newProxy(t testing.TB) *proxy {
	t.Helper()
	requireServer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: testAddr, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *proxy) Addr() string { return p.ln.Addr().String() }

func (p *proxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		down := p.down
		p.mu.Unlock()
		if down {
			_ = c.Close()
			continue
		}
		s, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.track(c, s)
		go p.pipe(c, s, true)
		go p.pipe(s, c, false)
	}
}

func (p *proxy) track(cs ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range cs {
		p.conns[c] = struct{}{}
	}
}

func (p *proxy) pipe(dst, src net.Conn, toClient bool) {
	defer func() {
		_ = dst.Close()
		_ = src.Close()
	}()
	if !toClient {
		_, _ = io.Copy(dst, src)
		return
	}
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if d := time.Duration(p.delay.Load()); d > 0 {
				time.Sleep(d)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// SetDelay holds every server response for d, keeping commands in flight.
func (p *proxy) SetDelay(d time.Duration) { p.delay.Store(int64(d)) }

// DropAll severs every open connection; new ones are still accepted.
func (p *proxy) DropAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}

// SetDown makes the "server" unreachable (true) or reachable again (false).
func (p *proxy) SetDown(down bool) {
	p.mu.Lock()
	p.down = down
	p.mu.Unlock()
	if down {
		p.DropAll()
	}
}

func (p *proxy) close() {
	_ = p.ln.Close()
	p.DropAll()
}
