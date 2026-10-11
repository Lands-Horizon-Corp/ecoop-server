//go:build integration

package cron_test

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cron"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/qns"
)

// Tests run against the valkey service in docker-compose.yml (the queue
// cron schedules on), configured from CACHE_* in the environment or the
// repo's .env. Each test gets its own queue namespace, so its jobs, ticks
// and scheduler leadership are its own.
var (
	valkeyAddr     string
	valkeyPassword string
	skipReason     string
)

var _ cron.CronServices = (*cron.CronService)(nil)

func TestMain(m *testing.M) {
	if err := startValkey(); err != nil {
		skipReason = "no Valkey: " + err.Error()
		fmt.Fprintln(os.Stderr, "cron tests:", skipReason)
	}
	os.Exit(m.Run())
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

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// newQueue is a started queue in t's namespace, through addr (Valkey or a
// proxy to it), stopped when t ends. Queues of one test share work like
// replicas of one app.
func newQueue(t testing.TB, addr ...string) queue.QueueServices {
	t.Helper()
	requireValkey(t)
	target := valkeyAddr
	if len(addr) > 0 {
		target = addr[0]
	}
	ch := cache.NewCacheService([]string{target}, valkeyPassword, nil)
	if err := ch.Run(context.Background()); err != nil {
		t.Fatalf("cache Run: %v", err)
	}
	q := queue.NewQueueService(ch, queue.Namespace(qns.For(t, valkeyAddr, valkeyPassword)))
	if err := q.Start(context.Background()); err != nil {
		t.Fatalf("queue Start: %v", err)
	}
	t.Cleanup(func() {
		_ = q.Stop(context.Background())
		_ = ch.Stop(context.Background())
	})
	return q
}

// newCron is a started cron service on a new queue, stopped when t ends.
func newCron(t testing.TB) (cron.CronServices, queue.QueueServices) {
	t.Helper()
	q := newQueue(t)
	c := cron.NewCronService(q, nil)
	if err := c.Start(ctxT(t)); err != nil {
		t.Fatalf("cron Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c, q
}

// recorder is a job handler that records every run.
type recorder struct {
	mu   sync.Mutex
	runs [][]byte
	n    atomic.Int64
}

func (r *recorder) handle(_ context.Context, payload []byte) error {
	r.mu.Lock()
	r.runs = append(r.runs, append([]byte(nil), payload...))
	r.mu.Unlock()
	r.n.Add(1)
	return nil
}

func (r *recorder) payloads() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]byte(nil), r.runs...)
}

// job is a valid job for t, with names unique to it.
func job(t testing.TB, schedule string, h queue.Handler) cron.CronJob {
	name := strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())
	return cron.CronJob{JobID: "job-" + name, Schedule: schedule, TaskName: "task-" + name, Payload: []byte("payload"), Handler: h}
}

// eventually polls cond until it holds or timeout passes.
func eventually(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
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

// proxy relays TCP to Valkey and can cut every connection.
type proxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns map[net.Conn]struct{}
	down  atomic.Bool
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
			if p.down.Load() {
				_ = c.Close()
				continue
			}
			s, err := net.Dial("tcp", valkeyAddr)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns[c], p.conns[s] = struct{}{}, struct{}{}
			p.mu.Unlock()
			go pipe(s, c)
			go pipe(c, s)
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); p.DropAll() })
	return p
}

func pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = dst.Close()
	_ = src.Close()
}

func (p *proxy) Addr() string { return p.ln.Addr().String() }

// Down refuses new connections and cuts the open ones until Up.
func (p *proxy) Down() { p.down.Store(true); p.DropAll() }
func (p *proxy) Up()   { p.down.Store(false) }

func (p *proxy) DropAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}
