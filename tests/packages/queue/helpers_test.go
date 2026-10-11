package queue_test

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

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/qns"
)

// Tests run against the valkey service in docker-compose.yml, using
// CACHE_HOST, CACHE_PORT and CACHE_PASSWORD from the environment or the
// repo's .env. Each test uses its own job type names and clears the queues
// when it ends.
var (
	valkeyAddr     string
	valkeyPassword string
	skipReason     string
)

var _ queue.QueueServices = (*queue.QueueService)(nil)

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	if err := startValkey(); err != nil {
		skipReason = "no Valkey: " + err.Error()
		fmt.Fprintln(os.Stderr, "queue tests:", skipReason)
	}
	return m.Run()
}

func startValkey() error {
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	env := map[string]string{}
	if data, err := os.ReadFile(filepath.Join(root, ".env")); err == nil {
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
	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker not installed")
	}
	up := exec.Command("docker", "compose", "up", "-d", "--wait", "valkey")
	up.Dir = root
	if out, err := up.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up valkey: %w\n%s", err, out)
	}
	return nil
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

func requireValkey(t testing.TB) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

// newCache returns a running cache on addr (Valkey, or a proxy to it).
func newCache(t testing.TB, addr string) cache.CacheServices {
	t.Helper()
	ch := cache.NewCacheService([]string{addr}, valkeyPassword, nil)
	if err := ch.Run(context.Background()); err != nil {
		t.Fatalf("cache Run: %v", err)
	}
	t.Cleanup(func() { _ = ch.Stop(context.Background()) })
	return ch
}

// ns is t's queue namespace: every queue a test starts shares it, and no
// other test sees it.
func ns(t testing.TB) string { return qns.For(t, valkeyAddr, valkeyPassword) }

// newQueue returns a started queue in t's namespace on its own cache
// connection, stopped when the test ends.
func newQueue(t testing.TB, addr ...string) queue.QueueServices {
	t.Helper()
	requireValkey(t)
	target := valkeyAddr
	if len(addr) > 0 {
		target = addr[0]
	}
	q := queue.NewQueueService(newCache(t, target), queue.Namespace(ns(t)))
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("queue Start: %v", err)
	}
	t.Cleanup(func() { _ = q.Stop(context.Background()) })
	return q
}

// archived is how many jobs on name gave up (ErrSkipRetry or out of retries).
func archived(t testing.TB, base string) int {
	t.Helper()
	in := qns.Inspector(valkeyAddr, valkeyPassword)
	defer in.Close()
	info, err := in.GetQueueInfo(queue.QueueName(ns(t), base))
	if err != nil {
		return 0
	}
	return info.Archived
}

var typeSeq atomic.Int64

var runID = time.Now().UnixNano()

// taskType returns a job type name unique to this test and to this run, so
// asynq state that outlives a run (Unique locks) never matches a later one.
func taskType(t testing.TB, name string) string {
	return fmt.Sprintf("test:%d:%d:%s", runID, typeSeq.Add(1), name)
}

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func eventually(t testing.TB, timeout time.Duration, cond func() bool, msg string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("not met within %v: "+msg, append([]any{timeout}, args...)...)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// never asserts cond stays false for d.
func never(t testing.TB, d time.Duration, cond func() bool, msg string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			t.Fatalf(msg, args...)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// recorder counts the jobs a handler sees and keeps their payloads.
type recorder struct {
	mu       sync.Mutex
	payloads []string
	times    []time.Time
	n        atomic.Int64
}

func (r *recorder) handler(err error) queue.Handler {
	return func(_ context.Context, payload []byte) error {
		r.mu.Lock()
		r.payloads = append(r.payloads, string(payload))
		r.times = append(r.times, time.Now())
		r.mu.Unlock()
		r.n.Add(1)
		return err
	}
}

func (r *recorder) count() int { return int(r.n.Load()) }

func (r *recorder) snapshot() ([]string, []time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.payloads...), append([]time.Time(nil), r.times...)
}

// proxy relays TCP to Valkey and can cut every connection.
type proxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newProxy(t testing.TB) *proxy {
	t.Helper()
	requireValkey(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, conns: map[net.Conn]struct{}{}}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s, err := net.Dial("tcp", valkeyAddr)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns[c], p.conns[s] = struct{}{}, struct{}{}
			p.mu.Unlock()
			go pipe(c, s)
			go pipe(s, c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); p.DropAll() })
	return p
}

func pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
}

func (p *proxy) Addr() string { return p.ln.Addr().String() }

func (p *proxy) DropAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
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
