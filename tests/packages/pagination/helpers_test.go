//go:build integration

package pagination_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// The suite runs against the compose Postgres (docker compose up postgres),
// configured from .env like the rest of the stack. One seeded database is
// shared by every read-only test; tests that write get a database of their
// own. The configured database itself is never touched.
var (
	pgAddr, pgUser, pgPassword, pgAdminDB string
	pgRun                                 = fmt.Sprintf("ecoop_pg_test_%d_%d", time.Now().Unix(), os.Getpid())
	admin                                 *sql.DB
	skipReason                            string
	shared                                *fixture
	dbSeq                                 atomic.Int64
)

// Account is the table every test filters. Email is nullable on purpose:
// it is the NULL / empty-string / dangling-pointer column.
type Account struct {
	bun.BaseModel `bun:"table:pg_accounts"`

	ID        int64     `bun:"id,pk,autoincrement"`
	Name      string    `bun:"name,notnull"`
	Email     *string   `bun:"email"`
	Tier      string    `bun:"tier,notnull"`
	Balance   float64   `bun:"balance,notnull"`
	Active    bool      `bun:"active,notnull"`
	Score     *int      `bun:"score"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

type fixture struct {
	sql ecsql.ISQLService
	svc pagination.IPaginationService[Account, int64]
	ids map[string]int64 // seeded name -> id
}

func TestMain(m *testing.M) {
	code := 0
	if err := startPostgres(); err != nil {
		skipReason = "no Postgres: " + err.Error()
		fmt.Fprintln(os.Stderr, "pagination tests:", skipReason)
		os.Exit(m.Run())
	}
	var cleanup func()
	var err error
	if shared, cleanup, err = buildFixture(seed()); err != nil {
		skipReason = "fixture: " + err.Error()
		fmt.Fprintln(os.Stderr, "pagination tests:", skipReason)
	}
	evCleanup, evErr := setupEvents()
	if evErr != nil {
		eventsSkip = "events fixture: " + evErr.Error()
		fmt.Fprintln(os.Stderr, "pagination tests:", eventsSkip)
	}
	code = m.Run()
	if evCleanup != nil {
		evCleanup()
	}
	if cleanup != nil {
		cleanup()
	}
	_ = admin.Close()
	os.Exit(code)
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
		if env[k] == "" {
			missing = append(missing, k)
		}
		return env[k]
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
	admin = sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsnFor(pgAdminDB)), pgdriver.WithReadTimeout(0)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		return fmt.Errorf("connecting to %s: %w", pgAddr, err)
	}
	return nil
}

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

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// need skips t when the suite has no database, and returns the shared one.
func need(t testing.TB) *fixture {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	return shared
}

// buildFixture creates a database, the accounts table, and inserts rows.
// openDB creates a database holding an empty table for model and returns a
// started service on it.
func openDB(model any) (ecsql.ISQLService, func(), error) {
	name := fmt.Sprintf("%s_%d", pgRun, dbSeq.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		return nil, nil, fmt.Errorf("create database: %w", err)
	}
	drop := func() {
		_, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	}
	dir, err := os.MkdirTemp("", "pagination-migrations-")
	if err != nil {
		drop()
		return nil, nil, err
	}
	f, err := os.Open(dir)
	if err != nil {
		drop()
		return nil, nil, err
	}
	s := ecsql.NewSQLService(dsnFor(name), 5, 20, f, false, &discard{}, []any{model}, nil)
	cleanup := func() {
		_ = s.Stop(context.Background())
		_ = f.Close()
		_ = os.RemoveAll(dir)
		drop()
	}
	if err := s.Start(ctx); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("start: %w", err)
	}
	if _, err := s.Client().NewCreateTable().Model(model).Exec(ctx); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("create table: %w", err)
	}
	return s, cleanup, nil
}

func buildFixture(rows []Account) (*fixture, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, cleanup, err := openDB((*Account)(nil))
	if err != nil {
		return nil, nil, err
	}
	db := s.Client()
	fx := &fixture{sql: s, ids: map[string]int64{}}
	if len(rows) > 0 {
		ptrs := make([]*Account, len(rows))
		for i := range rows {
			ptrs[i] = &rows[i]
		}
		if _, err := db.NewInsert().Model(&ptrs).Exec(ctx); err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("seed: %w", err)
		}
		for _, r := range ptrs {
			fx.ids[r.Name] = r.ID
		}
	}
	fx.svc = pagination.NewPaginationService(&pagination.PaginationService[Account, int64]{WriteSQLService: s}, utils.NewInflight())
	return fx, cleanup, nil
}

// ownFixture is a private, seeded database for tests that write or cut
// connections.
func ownFixture(t testing.TB, rows []Account) *fixture {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	fx, cleanup, err := buildFixture(rows)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	t.Cleanup(cleanup)
	return fx
}

type discard struct{}

func (*discard) Write(p []byte) (int, error) { return len(p), nil }

func ptr[T any](v T) *T { return &v }

var epoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// seed is the table every read-only test shares. Names are unique so a test
// can assert on them; the awkward ones (wildcards, quotes, NFD text, empty
// and NULL emails) are there to be filtered.
//
//	Alice    gold    100  active    alice@x.com   score 10
//	Bob      silver  200  inactive  bob@x.com     score 20
//	Carol    gold    0    active    NULL          score NULL
//	Dave_100% bronze 50   active    dave@x.com    score 30
//	O'Brien  silver  75.5 inactive  ""            score 40
//	Émile    gold    300  active    emile@x.com   score NULL   (precomposed é)
//	Frank    bronze  -20  inactive  NULL          score 50
//	Grace    gold    100  active    grace@x.com   score 10
//	Heidi    silver  100  active    heidi@x.com   score 20
//	Ivan     bronze  0    inactive  ""            score 30
func seed() []Account {
	mk := func(i int, name string, email *string, tier string, bal float64, active bool, score *int) Account {
		ts := epoch.AddDate(0, i, 0)
		return Account{Name: name, Email: email, Tier: tier, Balance: bal, Active: active, Score: score, CreatedAt: ts, UpdatedAt: ts}
	}
	return []Account{
		mk(0, "Alice", ptr("alice@x.com"), "gold", 100, true, ptr(10)),
		mk(1, "Bob", ptr("bob@x.com"), "silver", 200, false, ptr(20)),
		mk(2, "Carol", nil, "gold", 0, true, nil),
		mk(3, "Dave_100%", ptr("dave@x.com"), "bronze", 50, true, ptr(30)),
		mk(4, "O'Brien", ptr(""), "silver", 75.5, false, ptr(40)),
		mk(5, "Émile", ptr("emile@x.com"), "gold", 300, true, nil),
		mk(6, "Frank", nil, "bronze", -20, false, ptr(50)),
		mk(7, "Grace", ptr("grace@x.com"), "gold", 100, true, ptr(10)),
		mk(8, "Heidi", ptr("heidi@x.com"), "silver", 100, true, ptr(20)),
		mk(9, "Ivan", ptr(""), "bronze", 0, false, ptr(30)),
	}
}

func f(field string, mode pagination.Mode, value any) pagination.Filter {
	return pagination.Filter{Field: field, Mode: mode, Value: value}
}

func and(fs ...pagination.Filter) pagination.StructuredFilter {
	return pagination.StructuredFilter{Logic: pagination.LogicAnd, Filters: fs}
}

func or(fs ...pagination.Filter) pagination.StructuredFilter {
	return pagination.StructuredFilter{Logic: pagination.LogicOr, Filters: fs}
}

func asc(field string) pagination.SortField {
	return pagination.SortField{Field: field, Order: pagination.SortOrderAsc}
}
func desc(field string) pagination.SortField {
	return pagination.SortField{Field: field, Order: pagination.SortOrderDesc}
}

// names runs the filter through Filter and returns the matched names sorted
// by id, failing the test on error.
func names(t testing.TB, fx *fixture, sf pagination.StructuredFilter) []string {
	t.Helper()
	got, err := fx.svc.Filter(ctxT(t), sf)
	if err != nil {
		t.Fatalf("Filter(%+v): %v", sf.Filters, err)
	}
	return nameList(got)
}

func nameList(rows []*Account) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Name
	}
	return out
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[string]int{}
	for _, g := range got {
		seen[g]++
	}
	for _, w := range want {
		seen[w]--
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}

func wantSet(t testing.TB, got []string, want ...string) {
	t.Helper()
	if !sameSet(got, want) {
		t.Fatalf("matched %v, want %v", got, want)
	}
}

func filterWithSort(sf pagination.StructuredFilter, sorts ...pagination.SortField) pagination.StructuredFilter {
	sf.SortFields = sorts
	return sf
}

// page fetches one page of the shared table.
func page(t testing.TB, fx *fixture, sf pagination.StructuredFilter, size int, cursor *string) pagination.PaginationResult[Account] {
	t.Helper()
	res, err := fx.svc.Paginate(ctxT(t), pagination.Pagination{Filter: sf, PageSize: size, Cursor: cursor})
	if err != nil {
		t.Fatalf("Paginate: %v", err)
	}
	return res
}

// walk follows NextCursor from the first page and returns every name in order.
func walk(t testing.TB, fx *fixture, sf pagination.StructuredFilter, size int) []string {
	t.Helper()
	var out []string
	var cursor *string
	for i := 0; i < 1000; i++ {
		res := page(t, fx, sf, size, cursor)
		out = append(out, nameList(res.Data)...)
		if res.NextCursor == nil {
			return out
		}
		cursor = res.NextCursor
	}
	t.Fatal("pagination did not terminate: NextCursor never became nil")
	return nil
}

func mustNotPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s panicked: %v\n%s", name, r, debug.Stack())
		}
	}()
	fn()
}
