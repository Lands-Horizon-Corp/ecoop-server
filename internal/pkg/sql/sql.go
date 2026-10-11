package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/uptrace/bun/migrate"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

var _ SQLServices = (*SQLService)(nil)

type SQLService struct {
	dsn         string
	maxIdleConn int
	maxOpenConn int
	autoMigrate bool

	db         atomic.Pointer[bun.DB]
	sqldb      atomic.Pointer[sql.DB]
	migrations *os.File
	output     io.Writer
	models     []any

	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration

	lifecycle sync.Mutex // serialises Start and Stop
	gate      *utils.Gate
	inflight  *utils.Inflight
}

// NewSQLService creates a Postgres service. migrations is an opened
// directory holding goose .sql files; output receives Status and Version
// (os.Stdout if nil); models are the Bun models Migrate diffs against the
// database. inflight may be shared with other services; nil gives the
// service its own tracker.
func NewSQLService(
	dsn string,
	maxIdleConn int,
	maxOpenConn int,
	migrations *os.File,
	autoMigrate bool,
	output io.Writer,
	models []any,
	inflight *utils.Inflight,
) SQLServices {
	if output == nil {
		output = os.Stdout
	}
	if inflight == nil {
		inflight = utils.NewInflight()
	}
	return &SQLService{
		dsn:             dsn,
		maxIdleConn:     maxIdleConn,
		maxOpenConn:     maxOpenConn,
		autoMigrate:     autoMigrate,
		migrations:      migrations,
		output:          output,
		models:          models,
		connMaxLifetime: DefaultConnMaxLifetime,
		connMaxIdleTime: DefaultConnMaxIdleTime,
		gate:            utils.NewGate(inflight),
		inflight:        inflight,
	}
}

// Lifecycle

// Start opens the connection pool and checks the database answers. Calling
// it while started does nothing.
func (s *SQLService) Start(ctx context.Context) error {
	if s.dsn == "" {
		return ErrNoDSN
	}
	if s.maxOpenConn < 0 || s.maxIdleConn < 0 {
		return ErrInvalidPool
	}
	if s.gate == nil {
		return ErrNoTracker
	}
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.gate.IsOpen() {
		return nil
	}
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(s.dsn)))
	sqldb.SetMaxOpenConns(s.maxOpenConn)
	sqldb.SetMaxIdleConns(s.maxIdleConn) // database/sql caps it at MaxOpenConns
	sqldb.SetConnMaxLifetime(s.connMaxLifetime)
	sqldb.SetConnMaxIdleTime(s.connMaxIdleTime)
	db := bun.NewDB(sqldb, pgdialect.New())
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("sql: connecting: %w", err)
	}
	s.sqldb.Store(sqldb)
	s.db.Store(db)
	s.gate.Open()
	return nil
}

// Run applies pending migrations when autoMigrate is on, and does nothing
// otherwise. It only applies the files already in the migrations directory:
// generating them is Migrate's job, done in development so the files are
// reviewed and committed. Instances that Run at once take turns through a
// Postgres advisory lock.
func (s *SQLService) Run(ctx context.Context) error {
	if !s.autoMigrate {
		return nil
	}
	return s.up(ctx)
}

// Stop refuses new commands, waits for running ones (a migration is never
// cut off halfway), then closes the pool; database/sql lets queries already
// running through Client finish first. If ctx ends first Stop returns its
// error and closes the pool in the background once the commands finish.
func (s *SQLService) Stop(ctx context.Context) error {
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	db, sqldb := s.db.Load(), s.sqldb.Load()
	if err := s.gate.Close(ctx); err != nil {
		go func() {
			_ = s.inflight.Wait(context.Background())
			s.release(db, sqldb)
		}()
		return fmt.Errorf("sql: stop: commands still running: %w", err)
	}
	return s.release(db, sqldb)
}

// release closes db and clears it, unless a later Start replaced it.
func (s *SQLService) release(db *bun.DB, sqldb *sql.DB) error {
	if db == nil {
		return nil
	}
	s.db.CompareAndSwap(db, nil)
	s.sqldb.CompareAndSwap(sqldb, nil)
	if err := db.Close(); err != nil {
		return fmt.Errorf("sql: closing: %w", err)
	}
	return nil
}

func (s *SQLService) Ping(ctx context.Context) error {
	db, _, done, err := s.ready(false)
	if err != nil {
		return err
	}
	defer done()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("sql: ping: %w", err)
	}
	return nil
}

// Client returns the Bun handle, or nil before Start.
func (s *SQLService) Client() *bun.DB {
	return s.db.Load()
}

// Migrations

// Migrate generates a migration from the models, then applies every pending
// migration. Bun diffs the models against the live schema (goose's own table
// excluded) and the changes are written as a goose .sql file. If they drop a
// table or a column, the file gets ReviewSuffix instead and is not applied
// until a person renames it. When the models match the schema no file is
// written.
func (s *SQLService) Migrate(ctx context.Context) error {
	db, dir, done, err := s.ready(true)
	if err != nil {
		return err
	}
	defer done()
	return s.withMigrationLock(ctx, func() error {
		if len(s.models) > 0 {
			if err := s.generate(ctx, db, dir); err != nil {
				return err
			}
		}
		return s.upLocked(ctx)
	})
}

// Fresh drops everything in the current schema and reapplies all migrations.
// It destroys all data in that schema.
func (s *SQLService) Fresh(ctx context.Context) error {
	db, _, done, err := s.ready(true)
	if err != nil {
		return err
	}
	defer done()
	return s.withMigrationLock(ctx, func() error {
		var schema string
		if err := db.NewRaw("SELECT current_schema()").Scan(ctx, &schema); err != nil {
			return fmt.Errorf("sql: fresh: %w", err)
		}
		ident := bun.Ident(schema)
		if _, err := db.ExecContext(ctx, "DROP SCHEMA ? CASCADE", ident); err != nil {
			return fmt.Errorf("sql: fresh: dropping schema %q: %w", schema, err)
		}
		if _, err := db.ExecContext(ctx, "CREATE SCHEMA ?", ident); err != nil {
			return fmt.Errorf("sql: fresh: creating schema %q: %w", schema, err)
		}
		return s.upLocked(ctx)
	})
}

// Create writes a new goose .sql migration from the standard template, named
// name in snake_case, versioned after every existing migration.
func (s *SQLService) Create(ctx context.Context, name string) error {
	dir, err := s.dir()
	if err != nil {
		return err
	}
	slug := strings.Trim(nonSlug.ReplaceAllString(strings.ToLower(name), "_"), "_")
	if slug == "" {
		return ErrEmptyName
	}
	version, err := nextVersion(dir)
	if err != nil {
		return err
	}
	file := filepath.Join(dir, fmt.Sprintf("%d_%s.sql", version, slug))
	if err := os.WriteFile(file, []byte(migrationTemplate), 0o644); err != nil {
		return fmt.Errorf("sql: create %q: %w", name, err)
	}
	return nil
}

var nonSlug = regexp.MustCompile(`[^a-z0-9]+`)

const migrationTemplate = `-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
`

// Rollback reverts the last applied migration.
func (s *SQLService) Rollback(ctx context.Context) error {
	return s.withProvider(ctx, func(p *goose.Provider) error {
		_, err := p.Down(ctx)
		return noVersionOK(err)
	})
}

// RollbackTo reverts migrations until version is the latest applied.
func (s *SQLService) RollbackTo(ctx context.Context, version int64) error {
	return s.withProvider(ctx, func(p *goose.Provider) error {
		_, err := p.DownTo(ctx, version)
		return err
	})
}

// Redo reverts the last applied migration and applies it again.
func (s *SQLService) Redo(ctx context.Context) error {
	return s.withProvider(ctx, func(p *goose.Provider) error {
		if _, err := p.Down(ctx); err != nil {
			return err
		}
		_, err := p.UpByOne(ctx)
		return err
	})
}

// RollbackSteps reverts up to steps migrations, stopping early when none
// are left.
func (s *SQLService) RollbackSteps(ctx context.Context, steps int) error {
	if steps <= 0 {
		return ErrInvalidSteps
	}
	return s.withProvider(ctx, func(p *goose.Provider) error {
		for range steps {
			if _, err := p.Down(ctx); err != nil {
				return noVersionOK(err)
			}
		}
		return nil
	})
}

// UpSteps applies up to steps pending migrations, stopping early when none
// are left.
func (s *SQLService) UpSteps(ctx context.Context, steps int) error {
	if steps <= 0 {
		return ErrInvalidSteps
	}
	return s.withProvider(ctx, func(p *goose.Provider) error {
		for range steps {
			if _, err := p.UpByOne(ctx); err != nil {
				return noVersionOK(err)
			}
		}
		return nil
	})
}

// Status writes every migration and whether it is applied to the output.
func (s *SQLService) Status(ctx context.Context) error {
	return s.withProvider(ctx, func(p *goose.Provider) error {
		statuses, err := p.Status(ctx)
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(s.output, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "VERSION\tSTATE\tAPPLIED AT\tFILE")
		for _, st := range statuses {
			applied := "-"
			if !st.AppliedAt.IsZero() {
				applied = st.AppliedAt.Format(time.RFC3339)
			}
			fmt.Fprintf(w, "%d\t%s\t%s\t%s\n", st.Source.Version, st.State, applied, filepath.Base(st.Source.Path))
		}
		return w.Flush()
	})
}

// Version writes the latest applied migration version to the output.
func (s *SQLService) Version(ctx context.Context) error {
	return s.withProvider(ctx, func(p *goose.Provider) error {
		v, err := p.GetDBVersion(ctx)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(s.output, "version: %d\n", v)
		return err
	})
}

// Helpers

func (s *SQLService) up(ctx context.Context) error {
	_, _, done, err := s.ready(true)
	if err != nil {
		return err
	}
	defer done()
	return s.withMigrationLock(ctx, func() error { return s.upLocked(ctx) })
}

// upLocked applies pending migrations; the caller holds the migration lock.
func (s *SQLService) upLocked(ctx context.Context) error {
	p, err := s.provider(false)
	if errors.Is(err, goose.ErrNoMigrations) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("sql: applying migrations: %w", err)
	}
	return nil
}

// withProvider runs fn with a goose provider that takes the migration lock
// itself, so commands cannot interleave with another instance's migration.
func (s *SQLService) withProvider(ctx context.Context, fn func(*goose.Provider) error) error {
	_, _, done, err := s.ready(true)
	if err != nil {
		return err
	}
	defer done()
	p, err := s.provider(true)
	if err != nil {
		return err
	}
	if err := fn(p); err != nil {
		return fmt.Errorf("sql: %w", err)
	}
	return nil
}

// provider builds a goose provider over the migrations directory. It is
// rebuilt per command so files added by Create or Migrate are seen. Never
// Close it: goose's Provider.Close closes the shared *sql.DB.
func (s *SQLService) provider(locked bool) (*goose.Provider, error) {
	dir, err := s.dir()
	if err != nil {
		return nil, err
	}
	opts := []goose.ProviderOption{goose.WithDisableGlobalRegistry(true)}
	if locked {
		locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(MigrationLockID))
		if err != nil {
			return nil, err
		}
		opts = append(opts, goose.WithSessionLocker(locker))
	}
	p, err := goose.NewProvider(goose.DialectPostgres, s.sqldb.Load(), os.DirFS(dir), opts...)
	if err != nil {
		return nil, fmt.Errorf("sql: reading migrations in %s: %w", dir, err)
	}
	return p, nil
}

// withMigrationLock runs fn while holding the Postgres advisory lock, so
// generating and applying migrations happen on one instance at a time.
func (s *SQLService) withMigrationLock(ctx context.Context, fn func() error) error {
	sqldb := s.sqldb.Load()
	if sqldb == nil {
		return ErrNotStarted
	}
	conn, err := sqldb.Conn(ctx)
	if err != nil {
		return fmt.Errorf("sql: migration lock: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", MigrationLockID); err != nil {
		return fmt.Errorf("sql: migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", MigrationLockID)
	}()
	return fn()
}

// ready enters the gate and returns the database (and, when needDir, the
// migrations directory). The caller must call done when it finishes, so Stop
// waits for it.
func (s *SQLService) ready(needDir bool) (db *bun.DB, dir string, done func(), err error) {
	done, ok := s.gate.Enter()
	if !ok {
		return nil, "", nil, ErrNotStarted
	}
	db = s.db.Load()
	if db == nil {
		done()
		return nil, "", nil, ErrNotStarted
	}
	if needDir {
		if dir, err = s.dir(); err != nil {
			done()
			return nil, "", nil, err
		}
	}
	return db, dir, done, nil
}

func (s *SQLService) dir() (string, error) {
	if s.migrations == nil {
		return "", ErrNoMigrationsDir
	}
	dir := s.migrations.Name()
	info, err := os.Stat(dir)
	if err != nil {
		return "", fmt.Errorf("sql: migrations directory: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%w: %s", ErrNotADirectory, dir)
	}
	return dir, nil
}

// generate diffs the models against the schema and, if they differ, writes
// a goose migration. When the changes drop a table or column the whole
// migration is written with ReviewSuffix instead, which goose ignores, so no
// data is lost until a person reviews it and renames it to .sql.
func (s *SQLService) generate(ctx context.Context, db *bun.DB, dir string) error {
	scratch, err := os.MkdirTemp("", "bun-auto-*")
	if err != nil {
		return fmt.Errorf("sql: generating migration: %w", err)
	}
	defer os.RemoveAll(scratch)
	auto, err := migrate.NewAutoMigrator(db,
		migrate.WithModel(s.models...),
		migrate.WithExcludeTable(GooseTable),
		migrate.WithMigrationsDirectoryAuto(scratch),
	)
	if err != nil {
		return fmt.Errorf("sql: generating migration: %w", err)
	}
	files, err := auto.CreateSQLMigrations(ctx)
	if err != nil {
		return fmt.Errorf("sql: generating migration: %w", err)
	}
	if len(files) != 2 { // models match the schema
		return nil
	}
	up, down := lineComments(files[0].Content), lineComments(files[1].Content)
	body := "-- +goose Up\n" + up + "\n\n-- +goose Down\n" + down + "\n"

	if destructive.MatchString(up) {
		if pending, err := pendingReview(dir, body); err != nil || pending != "" {
			if pending != "" {
				fmt.Fprintf(s.output, "sql: destructive changes already waiting for review in %s\n", pending)
			}
			return err
		}
	}
	version, err := nextVersion(dir)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("%d_auto.sql", version)
	header := "-- Generated by Migrate from the models. Review before committing.\n"
	if destructive.MatchString(up) {
		name += ReviewSuffix
		header = "-- Generated by Migrate from the models. It DROPS tables or columns, so it\n" +
			"-- is not applied: review it, then rename it to end in .sql to apply it.\n"
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(header+body), 0o644); err != nil {
		return fmt.Errorf("sql: writing %s: %w", name, err)
	}
	if strings.HasSuffix(name, ReviewSuffix) {
		fmt.Fprintf(s.output, "sql: destructive changes written to %s for review; not applied\n", name)
	}
	return nil
}

// pendingReview returns the review file in dir that already holds body, so
// running Migrate again does not pile up copies of the same changes.
func pendingReview(dir, body string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*"+ReviewSuffix))
	if err != nil {
		return "", err
	}
	for _, m := range matches {
		content, err := os.ReadFile(m)
		if err != nil {
			return "", err
		}
		if strings.HasSuffix(string(content), body) {
			return filepath.Base(m), nil
		}
	}
	return "", nil
}

// lineComments turns /* ... */ blocks, which Bun uses for notes such as
// irreversible changes and goose cannot parse, into -- line comments.
func lineComments(sqlText string) string {
	var out []string
	inBlock := false
	for _, line := range strings.Split(strings.TrimSpace(sqlText), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case !inBlock && strings.HasPrefix(trimmed, "/*"):
			inBlock = !strings.HasSuffix(trimmed, "*/")
			if text := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "/*"), "*/")); text != "" {
				out = append(out, "-- "+text)
			}
		case inBlock:
			if strings.HasSuffix(trimmed, "*/") {
				inBlock = false
				trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, "*/"))
			}
			if trimmed != "" {
				out = append(out, "-- "+trimmed)
			}
		default:
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

// nextVersion is the current UTC time as a goose timestamp version, raised
// above every migration already in dir. Two generations in the same second
// (or a clock behind the newest file) would otherwise reuse a name and
// overwrite a migration, or break goose's ordering.
func nextVersion(dir string) (int64, error) {
	version, _ := strconv.ParseInt(time.Now().UTC().Format("20060102150405"), 10, 64)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("sql: reading migrations: %w", err)
	}
	for _, e := range entries {
		prefix, _, ok := strings.Cut(e.Name(), "_")
		if !ok {
			continue
		}
		if v, err := strconv.ParseInt(prefix, 10, 64); err == nil && v >= version {
			version = v + 1
		}
	}
	return version, nil
}

// destructive matches statements that drop a table or a column.
var destructive = regexp.MustCompile(`(?im)^\s*(DROP\s+TABLE|ALTER\s+TABLE\s+.+\s+DROP\s+COLUMN)\b`)

// noVersionOK treats "nothing left to apply or revert" as success.
func noVersionOK(err error) error {
	if errors.Is(err, goose.ErrNoNextVersion) || errors.Is(err, goose.ErrNoCurrentVersion) || errors.Is(err, goose.ErrNoMigrations) {
		return nil
	}
	return err
}
