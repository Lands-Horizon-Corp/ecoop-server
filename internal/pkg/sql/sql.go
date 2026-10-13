package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
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
) ISQLService {
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
	app, err := connector(withClientCheck(s.dsn))
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
	sqldb.SetMaxIdleConns(s.maxIdleConn)
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

// withClientCheck asks the server to notice, every second, that a client has
// gone away mid-query and abort the statement. pgdriver cancels by closing the
// socket, not by a cancel request, so without this a query whose caller timed
// out keeps running to completion. A DSN that sets the parameter itself, or
// that cannot be parsed, is left alone.
func withClientCheck(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		return dsn
	}
	q := u.Query()
	if q.Has("client_connection_check_interval") {
		return dsn
	}
	q.Set("client_connection_check_interval", "1000")
	u.RawQuery = q.Encode()
	return u.String()
}

func connector(dsn string, opts ...pgdriver.Option) (c *pgdriver.Connector, err error) {
	defer func() {
		if recover() != nil {
			c, err = nil, ErrInvalidDSN
		}
	}()
	return pgdriver.NewConnector(append([]pgdriver.Option{pgdriver.WithDSN(dsn)}, opts...)...), nil
}

func (s *SQLService) Run(ctx context.Context) error {
	if !s.autoMigrate {
		return nil
	}
	return s.migration(ctx, func(mig *sql.DB, dir string) error {
		return withMigrationLock(ctx, mig, func() error { return upLocked(ctx, mig, dir) })
	})
}

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

func (s *SQLService) Client() *bun.DB {
	return s.db.Load()
}

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
					if lockTimedOut(err) {
						return err
					}
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

func (s *SQLService) Fresh(ctx context.Context) error {
	return s.migration(ctx, func(mig *sql.DB, dir string) error {
		return withMigrationLock(ctx, mig, func() error {
			db := bun.NewDB(mig, pgdialect.New())
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

func (s *SQLService) Create(ctx context.Context, name string) error {
	if ctx == nil {
		return ErrNilContext
	}
	dir, err := s.dir()
	if err != nil {
		return err
	}
	slug := utils.Slug(name, '_')
	if slug == "" {
		return ErrEmptyName
	}
	select {
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

func (s *SQLService) Rollback(ctx context.Context) error {
	return s.withProvider(ctx, func(p *goose.Provider) error {
		_, err := p.Down(ctx)
		return noVersionOK(err)
	})
}

func (s *SQLService) RollbackTo(ctx context.Context, version int64) error {
	return s.withProvider(ctx, func(p *goose.Provider) error {
		_, err := p.DownTo(ctx, version)
		return err
	})
}

func (s *SQLService) Redo(ctx context.Context) error {
	return s.withProvider(ctx, func(p *goose.Provider) error {
		if _, err := p.Down(ctx); err != nil {
			return noVersionOK(err)
		}
		_, err := p.UpByOne(ctx)
		return err
	})
}

func (s *SQLService) RollbackSteps(ctx context.Context, steps int) error {
	return s.steps(ctx, steps, goose.StateApplied, func(p *goose.Provider) error {
		_, err := p.Down(ctx)
		return err
	})
}

func (s *SQLService) UpSteps(ctx context.Context, steps int) error {
	return s.steps(ctx, steps, goose.StatePending, func(p *goose.Provider) error {
		_, err := p.UpByOne(ctx)
		return err
	})
}

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

func upLocked(ctx context.Context, mig *sql.DB, dir string) error {
	p, err := provider(mig, dir)
	if errors.Is(err, goose.ErrNoMigrations) {
		return nil
	}
	if err != nil {
		return err
	}
	for attempt := 1; ; attempt++ {
		_, err := p.Up(ctx)
		if err == nil {
			return nil
		}
		if !lockTimedOut(err) || attempt == MigrationLockRetries {
			return fmt.Errorf("sql: applying migrations: %w", err)
		}
		select {
		case <-time.After(time.Duration(attempt) * time.Second):
		case <-ctx.Done():
			return fmt.Errorf("sql: applying migrations: %w", errors.Join(err, ctx.Err()))
		}
	}
}

func lockTimedOut(err error) bool {
	pg, ok := errors.AsType[pgdriver.Error](err)
	return ok && pg.Field('C') == "55P03"
}

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

func provider(mig *sql.DB, dir string) (*goose.Provider, error) {
	p, err := goose.NewProvider(goose.DialectPostgres, mig, os.DirFS(dir), goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return nil, fmt.Errorf("sql: reading migrations in %s: %w", dir, err)
	}
	return p, nil
}

func withMigrationLock(ctx context.Context, mig *sql.DB, fn func() error) error {
	conn, err := mig.Conn(ctx)
	if err != nil {
		return fmt.Errorf("sql: migration lock: %w", err)
	}
	defer conn.Close()
	for wait := 50 * time.Millisecond; ; wait = min(2*wait, time.Second) {
		var locked bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", MigrationLockID).Scan(&locked); err != nil {
			return fmt.Errorf("sql: migration lock: %w", err)
		}
		if locked {
			break
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return fmt.Errorf("sql: migration lock: %w", ctx.Err())
		}
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

func validModels(models []any) error {
	for i, m := range models {
		if t := reflect.TypeOf(m); t == nil || t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Struct {
			return fmt.Errorf("%w: models[%d] is %T, want a pointer to a struct", ErrInvalidModel, i, m)
		}
	}
	return nil
}

func writeNew(file, content string) error {
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, err = f.WriteString(content)
	return errors.Join(err, f.Close())
}

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

func noVersionOK(err error) error {
	if errors.Is(err, goose.ErrNoNextVersion) || errors.Is(err, goose.ErrNoCurrentVersion) || errors.Is(err, goose.ErrNoMigrations) {
		return nil
	}
	return err
}
