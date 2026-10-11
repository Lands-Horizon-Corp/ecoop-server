package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

var _ SQLServices = (*SQLService)(nil)

type SQLService struct {
	dsn         string
	maxIdleConn int
	maxOpenConn int
	autoMigrate bool

	db         atomic.Pointer[bun.DB] // the app's pool, returned by Client
	migdb      atomic.Pointer[sql.DB] // the pool migrations run on
	migrations *os.File
	output     io.Writer
	models     []any

	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration

	lifecycle sync.Mutex    // serialises Start and Stop
	migrating chan struct{} // one migration command at a time in this process
	gate      *utils.Gate
	inflight  *utils.Inflight
}

// NewSQLService creates a Postgres service. migrations is an opened
// directory holding goose .sql files; output receives Status and Version
// (os.Stdout if nil); models are pointers to the Bun models Migrate diffs
// against the database. inflight may be shared with other services; nil
// gives the service its own tracker.
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
		migrating:       make(chan struct{}, 1),
		gate:            utils.NewGate(inflight),
		inflight:        inflight,
	}
}

// Lifecycle

// Start opens the connection pools and checks the database answers. Calling
// it while started does nothing.
//
// Migrations get a pool of their own: app queries keep pgdriver's socket
// timeouts, while a migration statement (an index build, a backfill, a wait
// on a lock) runs as long as its ctx allows, and never waits for a
// connection held by app traffic.
func (s *SQLService) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if s.dsn == "" {
		return ErrNoDSN
	}
	if s.maxOpenConn < 0 || s.maxIdleConn < 0 {
		return ErrInvalidPool
	}
	if s.gate == nil {
		return ErrNoTracker
	}
	app, err := connector(s.dsn)
	if err != nil {
		return err
	}
	mig, _ := connector(s.dsn, pgdriver.WithReadTimeout(0))
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.gate.IsOpen() {
		return nil
	}
	sqldb := sql.OpenDB(app)
	sqldb.SetMaxOpenConns(s.maxOpenConn)
	sqldb.SetMaxIdleConns(s.maxIdleConn) // database/sql caps it at MaxOpenConns
	sqldb.SetConnMaxLifetime(s.connMaxLifetime)
	sqldb.SetConnMaxIdleTime(s.connMaxIdleTime)
	db := bun.NewDB(sqldb, pgdialect.New())
	migdb := sql.OpenDB(mig)
	migdb.SetConnMaxIdleTime(s.connMaxIdleTime)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		_ = migdb.Close()
		return fmt.Errorf("sql: connecting: %w", err)
	}
	s.migdb.Store(migdb)
	s.db.Store(db)
	s.gate.Open()
	return nil
}

// connector builds a pgdriver connector, which panics on a malformed DSN.
// The DSN holds the password, so the error does not repeat it.
func connector(dsn string, opts ...pgdriver.Option) (c *pgdriver.Connector, err error) {
	defer func() {
		if recover() != nil {
			c, err = nil, ErrInvalidDSN
		}
	}()
	return pgdriver.NewConnector(append([]pgdriver.Option{pgdriver.WithDSN(dsn)}, opts...)...), nil
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
	return s.migration(ctx, func(mig *sql.DB, dir string) error {
		return withMigrationLock(ctx, mig, func() error { return upLocked(ctx, mig, dir) })
	})
}

// Stop refuses new commands, waits for running ones (a migration is never
// cut off halfway), then closes the pools; database/sql lets queries already
// running through Client finish first. If ctx ends first Stop returns its
// error and closes the pools in the background once the commands finish.
func (s *SQLService) Stop(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	db, migdb := s.db.Load(), s.migdb.Load()
	if err := s.gate.Close(ctx); err != nil {
		go func() {
			_ = s.inflight.Wait(context.Background())
			s.release(db, migdb)
		}()
		return fmt.Errorf("sql: stop: commands still running: %w", err)
	}
	return s.release(db, migdb)
}

// release closes the pools and clears them, unless a later Start replaced
// them.
func (s *SQLService) release(db *bun.DB, migdb *sql.DB) error {
	if db == nil {
		return nil
	}
	s.db.CompareAndSwap(db, nil)
	s.migdb.CompareAndSwap(migdb, nil)
	if err := errors.Join(db.Close(), migdb.Close()); err != nil {
		return fmt.Errorf("sql: closing: %w", err)
	}
	return nil
}

func (s *SQLService) Ping(ctx context.Context) error {
	db, _, done, err := s.ready(ctx)
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

// Migrate applies pending migrations, then plans migrations from the
// models with Atlas and applies them until the schema matches (at most
// MigratePasses). Atlas compares the migrations folder, replayed in a
// scratch database, with the models' tables, created by Bun in another, so
// the plan never depends on what was changed by hand in this database; such
// a change is reported as ErrDrift instead. Applying first means a
// teammate's pending migration is never generated again.
//
// The models are the schema: tables, columns, foreign keys, and the indexes
// and checks their AfterCreateTable hooks create. A change that drops a
// table, column, check, foreign key or unique index is written with
// ReviewSuffix and not applied until a person renames it. A generated file
// that fails to apply is removed (goose rolled it back), so fixing the cause
// and running Migrate again starts clean.
func (s *SQLService) Migrate(ctx context.Context) error {
	if err := validModels(s.models); err != nil {
		return err
	}
	return s.migration(ctx, func(mig *sql.DB, dir string) error {
		return withMigrationLock(ctx, mig, func() error {
			if err := upLocked(ctx, mig, dir); err != nil || len(s.models) == 0 {
				return err
			}
			p, err := s.newPlanner(ctx, mig, dir)
			if err != nil {
				return err
			}
			defer p.close()
			for range MigratePasses {
				file, err := p.generate(ctx)
				if err != nil || file == "" {
					return err
				}
				if err := upLocked(ctx, mig, dir); err != nil {
					return errors.Join(ErrInvalidMigration, err, os.Remove(file))
				}
				if err := upLocked(ctx, p.folder, dir); err != nil {
					return err
				}
			}
			return ErrNotConverged
		})
	})
}

// Fresh drops everything in the current schema and reapplies all migrations.
// It destroys all data in that schema.
func (s *SQLService) Fresh(ctx context.Context) error {
	return s.migration(ctx, func(mig *sql.DB, dir string) error {
		return withMigrationLock(ctx, mig, func() error {
			db := bun.NewDB(mig, pgdialect.New()) // for quoting; never Close it
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
			return upLocked(ctx, mig, dir)
		})
	})
}

// Create writes a new goose .sql migration from the standard template, named
// name in snake_case, versioned after every existing migration. It needs no
// Start.
func (s *SQLService) Create(ctx context.Context, name string) error {
	if ctx == nil {
		return ErrNilContext
	}
	dir, err := s.dir()
	if err != nil {
		return err
	}
	slug := strings.Trim(nonSlug.ReplaceAllString(strings.ToLower(name), "_"), "_")
	if slug == "" {
		return ErrEmptyName
	}
	select { // so two Creates here never pick the same version
	case s.migrating <- struct{}{}:
		defer func() { <-s.migrating }()
	case <-ctx.Done():
		return ctx.Err()
	}
	version, err := nextVersion(dir)
	if err != nil {
		return err
	}
	if err := writeNew(filepath.Join(dir, fmt.Sprintf("%d_%s.sql", version, slug)), migrationTemplate); err != nil {
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

// Redo reverts the last applied migration and applies it again. With none
// applied there is nothing to redo.
func (s *SQLService) Redo(ctx context.Context) error {
	return s.withProvider(ctx, func(p *goose.Provider) error {
		if _, err := p.Down(ctx); err != nil {
			return noVersionOK(err)
		}
		_, err := p.UpByOne(ctx)
		return err
	})
}

// RollbackSteps reverts steps migrations. Asking for more than are applied
// is refused before any is reverted.
func (s *SQLService) RollbackSteps(ctx context.Context, steps int) error {
	return s.steps(ctx, steps, goose.StateApplied, func(p *goose.Provider) error {
		_, err := p.Down(ctx)
		return err
	})
}

// UpSteps applies steps pending migrations. Asking for more than are
// pending is refused before any is applied.
func (s *SQLService) UpSteps(ctx context.Context, steps int) error {
	return s.steps(ctx, steps, goose.StatePending, func(p *goose.Provider) error {
		_, err := p.UpByOne(ctx)
		return err
	})
}

// steps runs step steps times, after checking that many migrations are in
// state.
func (s *SQLService) steps(ctx context.Context, steps int, state goose.State, step func(*goose.Provider) error) error {
	if steps <= 0 {
		return ErrInvalidSteps
	}
	return s.withProvider(ctx, func(p *goose.Provider) error {
		statuses, err := p.Status(ctx)
		if err != nil {
			return err
		}
		n := 0
		for _, st := range statuses {
			if st.State == state {
				n++
			}
		}
		if steps > n {
			return fmt.Errorf("%w: %d asked, %d %s", ErrTooManySteps, steps, n, state)
		}
		for range steps {
			if err := step(p); err != nil {
				return err
			}
		}
		return nil
	})
}

// Status writes every migration and whether it is applied to the output.
func (s *SQLService) Status(ctx context.Context) error {
	w := tabwriter.NewWriter(s.output, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "VERSION\tSTATE\tAPPLIED AT\tFILE")
	err := s.withProvider(ctx, func(p *goose.Provider) error {
		statuses, err := p.Status(ctx)
		if err != nil {
			return err
		}
		for _, st := range statuses {
			applied := "-"
			if !st.AppliedAt.IsZero() {
				applied = st.AppliedAt.Format(time.RFC3339)
			}
			fmt.Fprintf(w, "%d\t%s\t%s\t%s\n", st.Source.Version, st.State, applied, filepath.Base(st.Source.Path))
		}
		return nil
	})
	if err != nil {
		return err
	}
	return w.Flush()
}

// Version writes the latest applied migration version to the output: 0
// when nothing has been applied.
func (s *SQLService) Version(ctx context.Context) error {
	_, mig, done, err := s.ready(ctx)
	if err != nil {
		return err
	}
	defer done()
	var v int64
	err = mig.QueryRowContext(ctx, "SELECT COALESCE(MAX(version_id), 0) FROM "+GooseTable).Scan(&v)
	if pg, ok := errors.AsType[pgdriver.Error](err); ok && pg.Field('C') == "42P01" { // no goose table yet
		err = nil
	}
	if err != nil {
		return fmt.Errorf("sql: version: %w", err)
	}
	_, err = fmt.Fprintf(s.output, "version: %d\n", v)
	return err
}

// Helpers

// ready checks ctx and enters the gate, returning the app and migration
// pools. The caller must call done when it finishes, so Stop waits for it.
func (s *SQLService) ready(ctx context.Context) (db *bun.DB, mig *sql.DB, done func(), err error) {
	if ctx == nil {
		return nil, nil, nil, ErrNilContext
	}
	done, ok := s.gate.Enter()
	if !ok {
		return nil, nil, nil, ErrNotStarted
	}
	db, mig = s.db.Load(), s.migdb.Load()
	if db == nil || mig == nil {
		done()
		return nil, nil, nil, ErrNotStarted
	}
	return db, mig, done, nil
}

// migration runs fn as this process's only migration command at the time:
// commands queue here (giving up when ctx ends) rather than each holding a
// connection while waiting for the database lock. fn gets the migration
// pool and the migrations directory.
func (s *SQLService) migration(ctx context.Context, fn func(mig *sql.DB, dir string) error) error {
	_, mig, done, err := s.ready(ctx)
	if err != nil {
		return err
	}
	defer done()
	dir, err := s.dir()
	if err != nil {
		return err
	}
	select {
	case s.migrating <- struct{}{}:
		defer func() { <-s.migrating }()
	case <-ctx.Done():
		return fmt.Errorf("sql: waiting for another migration command: %w", ctx.Err())
	}
	return fn(mig, dir)
}

// upLocked applies pending migrations; the caller holds the migration lock.
func upLocked(ctx context.Context, mig *sql.DB, dir string) error {
	p, err := provider(mig, dir)
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

// withProvider runs fn with a goose provider while holding the migration
// lock, so a command of several steps (Redo, RollbackSteps) cannot
// interleave with another instance's. With no migration files there is
// nothing to do.
func (s *SQLService) withProvider(ctx context.Context, fn func(*goose.Provider) error) error {
	return s.migration(ctx, func(mig *sql.DB, dir string) error {
		return withMigrationLock(ctx, mig, func() error {
			p, err := provider(mig, dir)
			if errors.Is(err, goose.ErrNoMigrations) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := fn(p); err != nil {
				return fmt.Errorf("sql: %w", err)
			}
			return nil
		})
	})
}

// provider builds a goose provider over dir; the caller holds the migration
// lock. It is rebuilt per command so files added by Create or Migrate are
// seen. Never Close it: goose's Provider.Close closes the shared *sql.DB.
func provider(mig *sql.DB, dir string) (*goose.Provider, error) {
	p, err := goose.NewProvider(goose.DialectPostgres, mig, os.DirFS(dir), goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return nil, fmt.Errorf("sql: reading migrations in %s: %w", dir, err)
	}
	return p, nil
}

// withMigrationLock runs fn while holding the Postgres advisory lock, so
// migration commands run on one instance at a time. The lock is held by a
// session, so a crashed instance's lock goes when its connection does.
func withMigrationLock(ctx context.Context, mig *sql.DB, fn func() error) error {
	conn, err := mig.Conn(ctx)
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

// validModels checks every model is a pointer to a struct, as Bun needs;
// anything else makes Bun panic.
func validModels(models []any) error {
	for i, m := range models {
		if t := reflect.TypeOf(m); t == nil || t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Struct {
			return fmt.Errorf("%w: models[%d] is %T, want a pointer to a struct", ErrInvalidModel, i, m)
		}
	}
	return nil
}

// writeNew creates file with content, failing rather than overwriting an
// existing migration.
func writeNew(file, content string) error {
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, err = f.WriteString(content)
	return errors.Join(err, f.Close())
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

// noVersionOK treats "nothing left to apply or revert" as success.
func noVersionOK(err error) error {
	if errors.Is(err, goose.ErrNoNextVersion) || errors.Is(err, goose.ErrNoCurrentVersion) || errors.Is(err, goose.ErrNoMigrations) {
		return nil
	}
	return err
}
