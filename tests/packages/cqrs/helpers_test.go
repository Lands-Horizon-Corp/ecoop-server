//go:build integration

package cqrs_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/suitelock"
)

// The suite runs against the compose stack, configured from .env or the
// environment (POSTGRES_*, POSTGRES_READ_*, BROKER_*, CDC_*). Every test gets its
// own pair of databases (a write one and a read one), named for this run, so
// tests run in parallel without seeing each other. The configured databases are
// never touched. Tests that need more than the two Postgres servers skip
// themselves when it is missing.
const testTopic = "test.public.accounts"

var (
	env        = map[string]string{}
	root       string
	runID      = time.Now().UnixNano()
	seqNo      atomic.Int64
	baseTime   = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	writeAdmin *sql.DB
	readAdmin  *sql.DB
	skipReason string
)

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	// The heavy suites share one Postgres: take turns with them.
	release, err := suitelock.Acquire("cqrs tests", 2*time.Hour)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cqrs tests:", err)
		return 1
	}
	defer release()
	if err := setup(); err != nil {
		skipReason = "no Postgres: " + err.Error()
		fmt.Fprintln(os.Stderr, "cqrs tests:", skipReason)
		return m.Run()
	}
	defer writeAdmin.Close()
	defer readAdmin.Close()
	dropStale(writeAdmin)
	dropStale(readAdmin)
	return m.Run()
}

func setup() error {
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
	root = dir
	if data, err := os.ReadFile(filepath.Join(dir, ".env")); err == nil {
		for line := range strings.SplitSeq(string(data), "\n") {
			if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok && !strings.HasPrefix(k, "#") {
				env[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
			}
		}
	}
	if err := compose("up", "-d", "--wait", "postgres", "postgres-read"); err != nil {
		return err
	}
	var err error
	if writeAdmin, err = openAdmin("POSTGRES", "5432", "ecoop"); err != nil {
		return err
	}
	if readAdmin, err = openAdmin("POSTGRES_READ", "5433", "ecoop_read"); err != nil {
		return err
	}
	return nil
}

func compose(args ...string) error {
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return nil
}

// cfg returns env[k] (the process environment wins), or def.
func cfg(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	if v := env[k]; v != "" {
		return v
	}
	return def
}

type server struct{ host, port, user, password, db string }

func serverOf(prefix, defPort, defDB string) server {
	return server{
		host: cfg(prefix+"_HOST", "127.0.0.1"), port: cfg(prefix+"_PORT", defPort),
		user: cfg(prefix+"_USER", "ecoop"), password: cfg(prefix+"_PASSWORD", "ecoop"),
		db: cfg(prefix+"_DB", defDB),
	}
}

func (s server) dsn(db string) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", s.user, s.password, net.JoinHostPort(s.host, s.port), db)
}

func openAdmin(prefix, defPort, defDB string) (*sql.DB, error) {
	s := serverOf(prefix, defPort, defDB)
	db := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(s.dsn(s.db)), pgdriver.WithReadTimeout(0)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connecting to %s: %w", net.JoinHostPort(s.host, s.port), err)
	}
	return db, nil
}

// dropStale removes the databases of runs that were killed before they could
// clean up, once they are older than any run could last.
func dropStale(admin *sql.DB) {
	rows, err := admin.Query(`SELECT datname FROM pg_database WHERE datname LIKE 'cqrs\_test\_%'`)
	if err != nil {
		return
	}
	var stale []string
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		parts := strings.Split(name, "_")
		if len(parts) < 4 {
			continue
		}
		if ns, err := strconv.ParseInt(parts[2], 10, 64); err == nil && time.Since(time.Unix(0, ns)) > 2*time.Hour {
			stale = append(stale, name)
		}
	}
	_ = rows.Close()
	for _, name := range stale {
		_, _ = admin.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, name))
	}
}

func skipUnlessPostgres(t testing.TB) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

// Account is the model under test: every column type the replicator handles.
type Account struct {
	bun.BaseModel `bun:"table:accounts"`

	ID        int64     `bun:"id,pk"`
	Owner     string    `bun:"owner"`
	Balance   int64     `bun:"balance"`
	Note      *string   `bun:"note"`
	Active    bool      `bun:"active"`
	UpdatedAt time.Time `bun:"updated_at,type:timestamptz"`
	Version   int64     `bun:"version"`
}

type AccountResponse struct {
	ID    int64  `json:"id"`
	Owner string `json:"owner"`
}

// Plain is the same table without a version column.
type Plain struct {
	bun.BaseModel `bun:"table:accounts"`

	ID      int64  `bun:"id,pk"`
	Owner   string `bun:"owner"`
	Balance int64  `bun:"balance"`
}

const accountsDDL = `CREATE TABLE accounts (
	id         bigint PRIMARY KEY,
	owner      text        NOT NULL DEFAULT '',
	balance    bigint      NOT NULL DEFAULT 0,
	note       text,
	active     boolean     NOT NULL DEFAULT false,
	updated_at timestamptz NOT NULL DEFAULT '1970-01-01T00:00:00Z',
	version    bigint      NOT NULL DEFAULT 0
)`

// pair is one test's databases and the services to reach them.
type pair struct {
	t        testing.TB
	write    ecsql.ISQLService
	read     ecsql.ISQLService
	writeDB  string
	readDB   string
	writeSrv server
	readSrv  server
}

// newPair creates a fresh write database and read database, each with the
// accounts table (the read one with the checkpoint table too), and starts a SQL
// service on each. Both are dropped when the test ends.
func newPair(t testing.TB, maxOpen int) *pair {
	t.Helper()
	skipUnlessPostgres(t)
	n := seqNo.Add(1)
	p := &pair{
		t:        t,
		writeSrv: serverOf("POSTGRES", "5432", "ecoop"),
		readSrv:  serverOf("POSTGRES_READ", "5433", "ecoop_read"),
	}
	p.writeDB = fmt.Sprintf("cqrs_test_%d_%d_w", runID, n)
	p.readDB = fmt.Sprintf("cqrs_test_%d_%d_r", runID, n)
	createDB(t, writeAdmin, p.writeDB)
	createDB(t, readAdmin, p.readDB)
	t.Cleanup(func() {
		_, _ = writeAdmin.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, p.writeDB))
		_, _ = readAdmin.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, p.readDB))
	})
	p.write = startSQL(t, p.writeSrv.dsn(p.writeDB), maxOpen)
	p.read = startSQL(t, p.readSrv.dsn(p.readDB), maxOpen)
	p.exec(p.write, accountsDDL)
	p.exec(p.write, `ALTER TABLE accounts REPLICA IDENTITY FULL`)
	p.exec(p.read, accountsDDL)
	p.exec(p.read, cqrs.CheckpointDDL)
	p.exec(p.read, cqrs.RowStateDDL)
	return p
}

func createDB(t testing.TB, admin *sql.DB, name string) {
	t.Helper()
	if _, err := admin.Exec(fmt.Sprintf(`CREATE DATABASE %q`, name)); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
}

func startSQL(t testing.TB, dsn string, maxOpen int) ecsql.ISQLService {
	t.Helper()
	svc := ecsql.NewSQLService(dsn, maxOpen, maxOpen, nil, false, nil, nil, nil)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("starting sql service: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc
}

func (p *pair) exec(svc ecsql.ISQLService, query string, args ...any) {
	p.t.Helper()
	if _, err := svc.Client().ExecContext(context.Background(), query, args...); err != nil {
		p.t.Fatalf("exec %q: %v", query, err)
	}
}

// rows is every account in the read database, by id.
func (p *pair) readAccounts() map[int64]Account {
	p.t.Helper()
	var rows []Account
	if err := p.read.Client().NewSelect().Model(&rows).Scan(context.Background()); err != nil {
		p.t.Fatalf("reading accounts: %v", err)
	}
	out := make(map[int64]Account, len(rows))
	for _, r := range rows {
		out[r.ID] = r
	}
	return out
}

func (p *pair) count(svc ecsql.ISQLService, table string) int {
	p.t.Helper()
	var n int
	if err := svc.Client().NewRaw("SELECT count(*) FROM "+table).Scan(context.Background(), &n); err != nil {
		p.t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// checkpoint is the stored next offset of a partition, or 0 when there is none.
func (p *pair) checkpoint(group string, partition int32) int64 {
	p.t.Helper()
	var next int64
	err := p.read.Client().NewRaw(`SELECT next_offset FROM cqrs_checkpoints WHERE consumer_group = ? AND topic = ? AND partition = ?`,
		group, testTopic, partition).Scan(context.Background(), &next)
	if errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	if err != nil {
		p.t.Fatalf("reading the checkpoint: %v", err)
	}
	return next
}

// noPagination is the pagination dependency NewCQRS insists on; the
// replication path never calls it.
func noPagination[T any](p *pair) pagination.IPaginationService[T, int64] {
	return pagination.NewPaginationService(&pagination.PaginationService[T, int64]{
		ReadSQLService: p.read, WriteSQLService: p.write,
	}, nil)
}

type cqrsSvc = cqrs.CQRSService[Account, AccountResponse, struct{}, int64]

// newService builds the CQRS service of the accounts table with a fake CDC.
func newService(t testing.TB, p *pair, f *fakeCDC, mods ...func(*cqrs.CQRSService[Account, AccountResponse, struct{}, int64])) cqrs.ICQRSService[Account, AccountResponse, struct{}, int64] {
	t.Helper()
	return newTyped[Account](t, p, f, "version", mods...)
}

func newTyped[T any](t testing.TB, p *pair, f *fakeCDC, version string, mods ...func(*cqrs.CQRSService[T, AccountResponse, struct{}, int64])) cqrs.ICQRSService[T, AccountResponse, struct{}, int64] {
	t.Helper()
	c := cqrs.CQRSService[T, AccountResponse, struct{}, int64]{
		Channel:           "accounts",
		Topic:             testTopic,
		ColumnVersion:     version,
		ReadSQLService:    p.read,
		WriteSQLService:   p.write,
		CDCService:        f,
		PaginationService: noPagination[T](p),
		Logger:            slog.New(slog.NewTextHandler(discard{}, nil)),
		BatchSize:         100,
		RetryBackoff:      10 * time.Millisecond,
		MaxRetryBackoff:   50 * time.Millisecond,
		ApplyTimeout:      10 * time.Second,
	}
	for _, m := range mods {
		m(&c)
	}
	return cqrs.NewCQRS(c, nil)
}

type discard struct{}

func (discard) Write(b []byte) (int, error) { return len(b), nil }

// start runs the service and stops it when the test ends.
func start[T any](t testing.TB, svc cqrs.ICQRSService[T, AccountResponse, struct{}, int64]) {
	t.Helper()
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = svc.Stop(ctx)
	})
}

// eventually polls cond until it holds or the timeout passes.
func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("never saw %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// recorder collects what the hooks report.
type recorder struct {
	mu       sync.Mutex
	payloads []AccountResponse
	events   int
}

func (r *recorder) dispatch(_ string, _ []string, p *AccountResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.payloads = append(r.payloads, *p)
	r.events++
	return nil
}

func (r *recorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.payloads, r.events = nil, 0
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.events
}
