package regressions

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// The SQL tests run against the Postgres in docker-compose.yml:
//
//	docker compose up -d --wait postgres
//
// SQL_TEST_DSN overrides the target. Each test gets its own throwaway database and a
// temp working directory, so the relative migrations directory never touches the repo.

const defaultPostgresDSN = "postgres://ecoop:ecoop-test-pass@localhost:5432/ecoop_test?sslmode=disable"

type sqlEnv struct {
	t      *testing.T
	dsn    string
	name   string
	dir    string
	db     *sql.DB // independent connection used only to inspect state
	status *os.File
}

// createTestDatabase makes an empty throwaway database and returns its name and DSN.
func createTestDatabase(t *testing.T) (name, dsn string) {
	t.Helper()
	adminDSN := envOr("SQL_TEST_DSN", defaultPostgresDSN)
	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("SQL_TEST_DSN: %v", err)
	}
	requireReachable(t, u.Host)

	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	name = fmt.Sprintf("regress_%d", time.Now().UnixNano())
	if _, err := admin.Exec(`CREATE DATABASE ` + name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`) })

	u.Path = "/" + name
	return name, u.String()
}

func (e *sqlEnv) openInspection() {
	e.t.Helper()
	db, err := sql.Open("pgx", e.dsn)
	if err != nil {
		e.t.Fatalf("open inspection connection: %v", err)
	}
	e.db = db
	e.t.Cleanup(func() { _ = db.Close() })
}

func newSQLEnv(t *testing.T) *sqlEnv {
	t.Helper()
	name, dsn := createTestDatabase(t)
	e := &sqlEnv{t: t, name: name, dsn: dsn}
	e.openInspection()

	t.Chdir(t.TempDir())
	e.dir = filepath.Join("src", "database", "migrations")
	if err := os.MkdirAll(e.dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var err error
	e.status, err = os.CreateTemp(t.TempDir(), "status-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.status.Close() })
	return e
}

// sibling is another empty database that shares this env's migrations directory and status output,
// like a second environment (canary, tenant) deployed from the same migration files.
func (e *sqlEnv) sibling() *sqlEnv {
	e.t.Helper()
	name, dsn := createTestDatabase(e.t)
	s := &sqlEnv{t: e.t, name: name, dsn: dsn, dir: e.dir, status: e.status}
	s.openInspection()
	return s
}

// dropDatabase removes the database out from under any connected service.
func (e *sqlEnv) dropDatabase() {
	e.t.Helper()
	admin, err := sql.Open("pgx", envOr("SQL_TEST_DSN", defaultPostgresDSN))
	if err != nil {
		e.t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(`DROP DATABASE ` + e.name + ` WITH (FORCE)`); err != nil {
		e.t.Fatalf("drop database: %v", err)
	}
}

// writeNext adds a hand-written goose migration whose version is just past every existing one.
func (e *sqlEnv) writeNext(name, up, down string) string {
	e.t.Helper()
	var max int64
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, entry := range entries {
		prefix, _, _ := strings.Cut(entry.Name(), "_")
		if v, err := strconv.ParseInt(prefix, 10, 64); err == nil && v > max {
			max = v
		}
	}
	path := filepath.Join(e.dir, fmt.Sprintf("%d_%s.sql", max+1, name))
	body := "-- +goose Up\n" + up + "\n\n-- +goose Down\n" + down + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		e.t.Fatal(err)
	}
	return path
}

// newService returns a service that has not been started.
func (e *sqlEnv) newService() sqlsvc.SQLServices {
	return sqlsvc.NewSQLService(e.dsn, 2, 8, e.status)
}

// running returns a started service that is stopped when the test ends.
func (e *sqlEnv) running() sqlsvc.SQLServices {
	e.t.Helper()
	svc := e.newService()
	if err := svc.Run(context.Background()); err != nil {
		e.t.Fatalf("run: %v", err)
	}
	e.t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	return svc
}

// write adds a hand-written goose migration with an explicit version number.
func (e *sqlEnv) write(version int64, name, up, down string) {
	e.t.Helper()
	body := "-- +goose Up\n" + up + "\n\n-- +goose Down\n" + down + "\n"
	path := filepath.Join(e.dir, fmt.Sprintf("%05d_%s.sql", version, name))
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		e.t.Fatal(err)
	}
}

func (e *sqlEnv) exec(query string, args ...any) {
	e.t.Helper()
	if _, err := e.db.Exec(query, args...); err != nil {
		e.t.Fatalf("exec %q: %v", query, err)
	}
}

func (e *sqlEnv) scanInt(query string, args ...any) int64 {
	e.t.Helper()
	var n int64
	if err := e.db.QueryRow(query, args...).Scan(&n); err != nil {
		e.t.Fatalf("query %q: %v", query, err)
	}
	return n
}

func (e *sqlEnv) hasTable(name string) bool {
	e.t.Helper()
	return e.scanInt(`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`, name) > 0
}

func (e *sqlEnv) hasColumn(table, column string) bool {
	e.t.Helper()
	return e.scanInt(`SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2`, table, column) > 0
}

// appliedVersion is the highest goose version currently applied (0 when none).
func (e *sqlEnv) appliedVersion() int64 {
	e.t.Helper()
	if !e.hasTable("goose_db_version") {
		return 0
	}
	return e.scanInt(`SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version WHERE is_applied`)
}

// schema is a stable text snapshot of every public table's columns, excluding goose's own table.
func (e *sqlEnv) schema() string {
	e.t.Helper()
	rows, err := e.db.Query(`
		SELECT table_name, column_name, data_type, is_nullable, COALESCE(character_maximum_length, 0), COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name <> 'goose_db_version'
		ORDER BY table_name, column_name`)
	if err != nil {
		e.t.Fatalf("snapshot schema: %v", err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var table, column, dataType, nullable, def string
		var maxLen int
		if err := rows.Scan(&table, &column, &dataType, &nullable, &maxLen, &def); err != nil {
			e.t.Fatal(err)
		}
		lines = append(lines, fmt.Sprintf("%s.%s %s null=%s len=%d default=%s", table, column, dataType, nullable, maxLen, def))
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// statusOutput returns everything Status and Version have printed so far.
func (e *sqlEnv) statusOutput() string {
	e.t.Helper()
	b, err := os.ReadFile(e.status.Name())
	if err != nil {
		e.t.Fatal(err)
	}
	return string(b)
}

func (e *sqlEnv) resetStatusOutput() {
	e.t.Helper()
	if err := e.status.Truncate(0); err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.status.Seek(0, 0); err != nil {
		e.t.Fatal(err)
	}
}

func (e *sqlEnv) migrationFiles() []string {
	e.t.Helper()
	files, err := filepath.Glob(filepath.Join(e.dir, "*.sql"))
	if err != nil {
		e.t.Fatal(err)
	}
	sort.Strings(files)
	return files
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

var bg = context.Background()

// filepathWalkFiles calls fn for every regular file under root.
func filepathWalkFiles(root string, fn func(path string)) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			fn(path)
		}
		return nil
	})
}
