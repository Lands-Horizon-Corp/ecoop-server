//go:build integration

package sql_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// The suite runs against the compose Postgres (docker compose up postgres),
// configured from .env like the rest of the stack. Every test gets its own
// database, named for this run, and its own migrations directory, so tests
// run in parallel without seeing each other (Postgres advisory locks are per
// database too). The configured database itself is never touched.
var (
	pgAddr     string // host:port
	pgUser     string
	pgPassword string
	pgAdminDB  string // POSTGRES_DB: where the suite creates and drops its databases from
	pgRun      = fmt.Sprintf("ecoop_test_%d_%d", time.Now().Unix(), os.Getpid())
	admin      *sql.DB
	skipReason string
)

var _ ecsql.SQLServices = (*ecsql.SQLService)(nil)

// suiteCleanups undo what fixtures shared by the whole run set up; they run
// after every test.
var suiteCleanups []func()

func TestMain(m *testing.M) { os.Exit(runMain(m)) }

func runMain(m *testing.M) (code int) {
	if err := startPostgres(); err != nil {
		skipReason = "no Postgres: " + err.Error()
		fmt.Fprintln(os.Stderr, "sql tests:", skipReason)
		return m.Run()
	}
	defer admin.Close()
	scratchBefore := scratchDatabases()
	code = m.Run()
	for _, cleanup := range slices.Backward(suiteCleanups) {
		cleanup()
	}
	// Migrate's scratch databases must never be left behind, failures included.
	var left []string
	for _, name := range scratchDatabases() {
		if !slices.Contains(scratchBefore, name) {
			left = append(left, name)
		}
	}
	if len(left) > 0 {
		fmt.Fprintln(os.Stderr, "sql tests: leaked scratch databases:", left)
		code = 1
	}
	return code
}

func startPostgres() error {
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
	host, port := get("POSTGRES_HOST"), get("POSTGRES_PORT")
	pgUser, pgPassword, pgAdminDB = get("POSTGRES_USER"), get("POSTGRES_PASSWORD"), get("POSTGRES_DB")
	if len(missing) > 0 {
		return fmt.Errorf("set %s in .env or the environment", strings.Join(missing, ", "))
	}
	pgAddr = net.JoinHostPort(host, port)
	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker not installed")
	}
	up := exec.Command("docker", "compose", "up", "-d", "--wait", "postgres")
	up.Dir = root
	if out, err := up.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up postgres: %w\n%s", err, out)
	}
	// Test setup only: CREATE DATABASE queues behind every other one on the
	// server, so it is bounded by a context rather than a socket timeout.
	admin = sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsnFor(pgAdminDB)), pgdriver.WithReadTimeout(0)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		return fmt.Errorf("connecting to %s: %w", pgAddr, err)
	}
	dropStaleDatabases()
	return nil
}

// dropStaleDatabases drops test databases a killed run left behind, once
// they are older than any run could last. Only names this suite creates
// are touched.
func dropStaleDatabases() {
	rows, err := admin.Query("SELECT datname FROM pg_database WHERE datname LIKE 'ecoop\\_test\\_%'")
	if err != nil {
		return
	}
	var stale []string
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		parts := strings.Split(name, "_")
		if len(parts) < 3 {
			continue
		}
		if sec, err := strconv.ParseInt(parts[2], 10, 64); err == nil && time.Since(time.Unix(sec, 0)) > time.Hour {
			stale = append(stale, name)
		}
	}
	rows.Close()
	for _, name := range stale {
		_, _ = admin.Exec(`DROP DATABASE IF EXISTS "` + name + `" WITH (FORCE)`)
	}
}

func scratchDatabases() []string {
	var out []string
	rows, err := admin.Query("SELECT datname FROM pg_database WHERE datname LIKE 'sql\\_scratch\\_%'")
	if err != nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		out = append(out, name)
	}
	return out
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
	for _, k := range []string{"POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"} {
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

func dsnFor(db string) string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword(pgUser, pgPassword), Host: pgAddr, Path: "/" + db, RawQuery: "sslmode=disable"}
	return u.String()
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
	name := fmt.Sprintf("%s_%d", pgRun, dbSeq.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
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
	return slices.Contains(tables(t, s), name)
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

func ctxShort(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// dbName is the database name in a DSN built by dsnFor.
func dbName(dsn string) string {
	rest := dsn[strings.LastIndex(dsn, "/")+1:]
	name, _, _ := strings.Cut(rest, "?")
	return name
}

// snapshot describes the current schema: columns, indexes, constraints
// (NOT NULL ones are in the columns), views, triggers, functions, enum
// types, partitions and partition keys, sequence owners, extended
// statistics, storage options, invalid indexes and extensions, goose's
// table excluded. Two
// databases with the same snapshot have the same schema.
func snapshot(t testing.TB, s ecsql.SQLServices) string {
	t.Helper()
	snap, err := snapshotOf(ctxT(t), s.Client())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	return snap
}

func snapshotOf(ctx context.Context, db *bun.DB) (string, error) {
	const q = `
SELECT 'col ' || table_name || '.' || column_name || ' ' || data_type || coalesce('(' || character_maximum_length || ')', '') ||
       coalesce(' p' || numeric_precision || ',' || numeric_scale, '') || ' null=' || is_nullable || ' def=' || coalesce(column_default, '-')
  FROM information_schema.columns WHERE table_schema = current_schema() AND table_name <> 'goose_db_version'
UNION ALL
SELECT 'idx ' || indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename <> 'goose_db_version'
UNION ALL
SELECT 'con ' || c.conrelid::regclass || ' ' || c.conname || ' ' || pg_get_constraintdef(c.oid)
  FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
 WHERE n.nspname = current_schema() AND c.conrelid::regclass::text <> 'goose_db_version' AND c.contype <> 'n'
UNION ALL
SELECT 'view ' || viewname || ' ' || definition FROM pg_views WHERE schemaname = current_schema()
UNION ALL
SELECT 'trg ' || tgrelid::regclass || ' ' || tgname FROM pg_trigger WHERE NOT tgisinternal
UNION ALL
SELECT 'fn ' || pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = current_schema()
UNION ALL
SELECT 'type ' || t.typname || ' ' || string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder)
  FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid JOIN pg_namespace n ON n.oid = t.typnamespace
 WHERE n.nspname = current_schema() GROUP BY t.typname
UNION ALL
SELECT 'part ' || c.relname || ' ' || pg_get_expr(c.relpartbound, c.oid)
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relispartition AND c.relkind IN ('r', 'p')
UNION ALL
SELECT 'partkey ' || c.relname || ' ' || pg_get_partkeydef(c.oid)
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relkind = 'p'
UNION ALL
SELECT 'seq ' || s.relname || ' owned by ' || coalesce(t.relname || '.' || a.attname, '-')
  FROM pg_class s JOIN pg_namespace n ON n.oid = s.relnamespace
  LEFT JOIN pg_depend d ON d.objid = s.oid AND d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')
  LEFT JOIN pg_class t ON t.oid = d.refobjid LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
 WHERE s.relkind = 'S' AND n.nspname = current_schema() AND coalesce(t.relname, '') <> 'goose_db_version'
UNION ALL
SELECT 'stat ' || x.stxname || ' ' || pg_get_statisticsobjdef(x.oid)
  FROM pg_statistic_ext x JOIN pg_namespace n ON n.oid = x.stxnamespace WHERE n.nspname = current_schema()
UNION ALL
SELECT 'opt ' || c.relname || ' ' || array_to_string(c.reloptions, ',')
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.reloptions IS NOT NULL
UNION ALL
SELECT 'invalid ' || c.relname
  FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = current_schema() AND NOT i.indisvalid
UNION ALL
SELECT 'ext ' || extname FROM pg_extension WHERE extname <> 'plpgsql'
ORDER BY 1`
	var rows []string
	if err := db.NewRaw(q).Scan(ctx, &rows); err != nil {
		return "", err
	}
	return strings.Join(rows, "\n"), nil
}

// count returns the rows of a SQL count query.
func count(t testing.TB, s ecsql.SQLServices, query string, args ...any) int {
	t.Helper()
	var n int
	if err := s.Client().NewRaw(query, args...).Scan(ctxT(t), &n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func execSQL(t testing.TB, s ecsql.SQLServices, query string, args ...any) {
	t.Helper()
	if _, err := s.Client().ExecContext(ctxT(t), query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// diffLines shows what differs between two snapshots.
func diffLines(a, b string) string {
	in := func(s string) map[string]bool {
		m := map[string]bool{}
		for l := range strings.SplitSeq(s, "\n") {
			m[l] = true
		}
		return m
	}
	ma, mb := in(a), in(b)
	var out []string
	for l := range ma {
		if !mb[l] {
			out = append(out, "- "+l)
		}
	}
	for l := range mb {
		if !ma[l] {
			out = append(out, "+ "+l)
		}
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

// pgArray passes ids as a Postgres array parameter.
func pgArray(ids []int64) any { return pgdialect.Array(ids) }

// seed runs a bulk statement (test data) on its own connection to e's
// database. The app's pool keeps pgdriver's 10s read timeout on purpose;
// loading 100,000 rows per table can take longer when the whole suite
// shares the server, so seeding is bounded by the test's context instead.
func seed(t testing.TB, e *env, query string) {
	t.Helper()
	db := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(e.dsn), pgdriver.WithReadTimeout(0)))
	defer db.Close()
	if _, err := db.ExecContext(ctxT(t), query); err != nil {
		t.Fatalf("seeding: %v", err)
	}
}
