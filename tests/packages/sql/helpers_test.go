package sql_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uptrace/bun/driver/pgdriver"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// The suite runs against a throwaway Postgres 18 container that TestMain
// starts and removes. Every test gets its own database and its own
// migrations directory, so tests run in parallel without seeing each other
// (Postgres advisory locks are per database too).
var (
	pgAddr     string // host:port of the container
	admin      *sql.DB
	skipReason string
)

const pgPassword = "sql-test-pw"

var _ ecsql.SQLServices = (*ecsql.SQLService)(nil)

func TestMain(m *testing.M) { os.Exit(runMain(m)) }

func runMain(m *testing.M) int {
	stop, err := startPostgres()
	if err != nil {
		skipReason = "no Postgres: " + err.Error()
		fmt.Fprintln(os.Stderr, "sql tests:", skipReason)
		return m.Run()
	}
	defer stop()
	return m.Run()
}

func startPostgres() (stop func(), err error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, errors.New("docker not installed")
	}
	out, err := exec.Command("docker", "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD="+pgPassword,
		"-p", "127.0.0.1::5432", "postgres:18-alpine", "-c", "max_connections=500").Output()
	if err != nil {
		return nil, fmt.Errorf("docker run: %w", err)
	}
	id := strings.TrimSpace(string(out))
	stop = func() {
		if admin != nil {
			_ = admin.Close()
		}
		_ = exec.Command("docker", "rm", "-f", id).Run()
	}
	port, err := exec.Command("docker", "port", id, "5432/tcp").Output()
	if err != nil {
		stop()
		return nil, fmt.Errorf("docker port: %w", err)
	}
	pgAddr = strings.TrimSpace(strings.Split(string(port), "\n")[0])
	admin = sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsnFor("postgres"))))
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := admin.PingContext(ctx)
		cancel()
		if err == nil {
			return stop, nil
		}
		if time.Now().After(deadline) {
			stop()
			return nil, fmt.Errorf("postgres never became ready: %w", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func dsnFor(db string) string {
	return fmt.Sprintf("postgres://postgres:%s@%s/%s?sslmode=disable", pgPassword, pgAddr, db)
}

func requirePG(t testing.TB) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

var dbSeq atomic.Int64

// newDatabase creates an empty database for t and returns its DSN. It is
// dropped when the test ends.
func newDatabase(t testing.TB) string {
	t.Helper()
	requirePG(t)
	name := fmt.Sprintf("t_%d", dbSeq.Add(1))
	if _, err := admin.ExecContext(context.Background(), "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	return dsnFor(name)
}

// migrationsDir returns an opened, empty migrations directory for t.
func migrationsDir(t testing.TB) (*os.File, string) {
	t.Helper()
	path := t.TempDir()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f, path
}

// env is one test's database, migrations directory and output.
type env struct {
	dsn  string
	dir  *os.File
	path string
	out  *syncBuffer
}

func newEnv(t testing.TB) *env {
	t.Helper()
	dir, path := migrationsDir(t)
	return &env{dsn: newDatabase(t), dir: dir, path: path, out: &syncBuffer{}}
}

// service builds (not started) a service on e with the given models.
func (e *env) service(models ...any) ecsql.SQLServices {
	return ecsql.NewSQLService(e.dsn, 5, 10, e.dir, false, e.out, models, nil)
}

// started builds and starts a service, stopped when the test ends.
func (e *env) started(t testing.TB, models ...any) ecsql.SQLServices {
	t.Helper()
	s := e.service(models...)
	if err := s.Start(ctxT(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s
}

func (e *env) files(pattern string) []string {
	m, _ := filepath.Glob(filepath.Join(e.path, pattern))
	sort.Strings(m)
	return m
}

// write adds a hand-written goose migration with the given version.
func (e *env) write(t testing.TB, version int64, name, up, down string) {
	t.Helper()
	body := fmt.Sprintf("-- +goose Up\n%s\n\n-- +goose Down\n%s\n", up, down)
	if err := os.WriteFile(filepath.Join(e.path, fmt.Sprintf("%d_%s.sql", version, name)), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func tables(t testing.TB, s ecsql.SQLServices) []string {
	t.Helper()
	var out []string
	err := s.Client().NewRaw("SELECT tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename <> ? ORDER BY 1", ecsql.GooseTable).Scan(ctxT(t), &out)
	if err != nil {
		t.Fatalf("listing tables: %v", err)
	}
	return out
}

func hasTable(t testing.TB, s ecsql.SQLServices, name string) bool {
	for _, tb := range tables(t, s) {
		if tb == name {
			return true
		}
	}
	return false
}

func columns(t testing.TB, s ecsql.SQLServices, table string) []string {
	t.Helper()
	var out []string
	err := s.Client().NewRaw("SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ? ORDER BY 1", table).Scan(ctxT(t), &out)
	if err != nil {
		t.Fatalf("listing columns: %v", err)
	}
	return out
}

// version returns the applied version Version prints.
func version(t testing.TB, s ecsql.SQLServices, e *env) string {
	t.Helper()
	e.out.Reset()
	if err := s.Version(ctxT(t)); err != nil {
		t.Fatalf("Version: %v", err)
	}
	return strings.TrimSpace(strings.TrimPrefix(e.out.String(), "version: "))
}

// syncBuffer is a bytes.Buffer safe for the concurrent writes of a shared
// output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
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

// proxy relays TCP to Postgres and can cut every connection.
type proxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newProxy(t testing.TB) *proxy {
	t.Helper()
	requirePG(t)
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
			s, err := net.Dial("tcp", pgAddr)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns[c], p.conns[s] = struct{}{}, struct{}{}
			p.mu.Unlock()
			go func() { _, _ = io.Copy(s, c); _ = s.Close(); _ = c.Close() }()
			go func() { _, _ = io.Copy(c, s); _ = c.Close(); _ = s.Close() }()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); p.DropAll() })
	return p
}

func (p *proxy) Addr() string { return p.ln.Addr().String() }

// DSN rewrites dsn to go through the proxy.
func (p *proxy) DSN(dsn string) string { return strings.Replace(dsn, pgAddr, p.Addr(), 1) }

func (p *proxy) DropAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}
