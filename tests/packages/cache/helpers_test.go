package cache_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	valkey "github.com/valkey-io/valkey-go"
)

// Server-backed tests need a disposable Valkey/Redis. By default TestMain
// starts one in Docker on a random port. To use your own instead, set
// CACHE_TEST_ADDR (and CACHE_TEST_PASSWORD); Flush tests then only run when
// CACHE_TEST_ALLOW_FLUSH=1 because they wipe the whole database.
var (
	testAddr     string
	testPassword string
	allowFlush   bool
	skipReason   string
)

var _ cache.CacheServices = (*cache.CacheService)(nil)

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	if addr := os.Getenv("CACHE_TEST_ADDR"); addr != "" {
		testAddr, testPassword = addr, os.Getenv("CACHE_TEST_PASSWORD")
		allowFlush = os.Getenv("CACHE_TEST_ALLOW_FLUSH") == "1"
		return m.Run()
	}
	stop, err := startContainer()
	if err != nil {
		skipReason = "no test server: " + err.Error()
		fmt.Fprintln(os.Stderr, "cache tests:", skipReason)
		return m.Run()
	}
	defer stop()
	allowFlush = true
	return m.Run()
}

func startContainer() (stop func(), err error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, errors.New("docker not installed and CACHE_TEST_ADDR unset")
	}
	testPassword = "cache-test-pw"
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", "127.0.0.1::6379",
		"redis:8-alpine", "redis-server", "--requirepass", testPassword, "--save", "", "--appendonly", "no").Output()
	if err != nil {
		return nil, fmt.Errorf("docker run: %w", err)
	}
	id := strings.TrimSpace(string(out))
	stop = func() { _ = exec.Command("docker", "rm", "-f", id).Run() }

	port, err := exec.Command("docker", "port", id, "6379/tcp").Output()
	if err != nil {
		stop()
		return nil, fmt.Errorf("docker port: %w", err)
	}
	testAddr = strings.TrimSpace(strings.Split(string(port), "\n")[0])

	deadline := time.Now().Add(20 * time.Second)
	for {
		svc := cache.NewCacheService(testPassword, []string{testAddr}, nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := svc.Run(ctx)
		cancel()
		_ = svc.Stop(context.Background())
		if err == nil {
			return stop, nil
		}
		if time.Now().After(deadline) {
			stop()
			return nil, fmt.Errorf("server never became ready: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func requireServer(t testing.TB) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

func requireFlush(t testing.TB) {
	t.Helper()
	requireServer(t)
	if !allowFlush {
		t.Skip("set CACHE_TEST_ALLOW_FLUSH=1 to run tests that flush the database")
	}
}

func newRunning(t testing.TB) cache.CacheServices {
	t.Helper()
	return newRunningAt(t, testAddr)
}

func newRunningAt(t testing.TB, addr string) cache.CacheServices {
	t.Helper()
	requireServer(t)
	svc := cache.NewCacheService(testPassword, []string{addr}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc
}

// key namespaces every key by test name so tests never see each other's data.
func key(t testing.TB, parts ...string) string {
	return "test:" + t.Name() + ":" + strings.Join(parts, ":")
}

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func pttl(t testing.TB, svc cache.CacheServices, k string) int64 {
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
