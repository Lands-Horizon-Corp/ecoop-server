package database

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/go-playground/validator/v10"
)

type DatabaseService struct {
	started  atomic.Bool // read by every request, flipped by Start/Stop
	registry map[reflect.Type]*registration

	// cancelRunners and runners let Stop end the model runners before it
	// closes the connections they use.
	cancelRunners context.CancelFunc
	runners       sync.WaitGroup

	// readerSQL and writerSQL belong to Start and Stop (which never run concurrently with each
	// other); requests read the published pair in conns, which Start replaces atomically.
	readerSQL sql.SQLServices
	writerSQL sql.SQLServices
	conns     atomic.Pointer[connPair]

	readerLogger logger.LogContextService
	writerLogger logger.LogContextService
	cqrsLogger   logger.LogContextService

	writerDsn string
	readerDsn string

	maxIdleConn int
	maxOpenConn int

	migrations  *os.File
	autoMigrate bool
	output      io.Writer
	models      []any

	broadcast     broadcast.BroadcasterServices
	messageBroker broker.MessageBrokerServices
	Validator     *validator.Validate

	BatchSize     int
	FlushInterval time.Duration

	sqlOpts   []sql.Option
	pgbouncer bool
}

func NewDatabaseService(
	writerDsn string,
	readerDsn string,

	maxIdleConn int,
	maxOpenConn int,

	readerLogger logger.LogContextService,
	writerLogger logger.LogContextService,
	cqrsLogger logger.LogContextService,

	migrations *os.File,
	autoMigrate bool,
	output io.Writer,
	models []any,

	broadcast broadcast.BroadcasterServices,
	messageBroker broker.MessageBrokerServices,
	validator *validator.Validate,

	batchSize int,
	flushInterval time.Duration,
	opts ...Option,
) *DatabaseService {
	db := &DatabaseService{
		registry:      make(map[reflect.Type]*registration),
		writerDsn:     writerDsn,
		readerDsn:     readerDsn,
		readerLogger:  readerLogger,
		writerLogger:  writerLogger,
		cqrsLogger:    cqrsLogger,
		maxIdleConn:   maxIdleConn,
		maxOpenConn:   maxOpenConn,
		migrations:    migrations,
		autoMigrate:   autoMigrate,
		output:        output,
		models:        models,
		broadcast:     broadcast,
		messageBroker: messageBroker,
		Validator:     validator,
		BatchSize:     batchSize,
		FlushInterval: flushInterval,
	}
	for _, opt := range opts {
		opt(db)
	}
	if db.pgbouncer {
		db.writerDsn = withDSNParam(db.writerDsn, "default_query_exec_mode", "exec")
		db.readerDsn = withDSNParam(db.readerDsn, "default_query_exec_mode", "exec")
	}
	return db
}

func (db *DatabaseService) Start(ctx context.Context) error {
	if db.pgbouncer && db.autoMigrate && db.migrations != nil {
		return ErrMigrateThroughPooler
	}
	db.Stop(ctx)
	db.writerSQL = sql.NewSQLService(
		db.writerDsn,
		db.maxIdleConn,
		db.maxOpenConn,
		db.migrations,
		db.autoMigrate,
		db.output,
		db.models,
		db.writerLogger,
		db.sqlOpts...,
	)
	db.readerSQL = sql.NewSQLService(
		db.readerDsn,
		db.maxIdleConn,
		db.maxOpenConn,
		db.migrations,
		db.autoMigrate,
		db.output,
		db.models,
		db.readerLogger,
		db.sqlOpts...,
	)
	db.conns.Store(&connPair{writer: db.writerSQL, reader: db.readerSQL})
	if err := db.writerSQL.Run(ctx); err != nil {
		return fmt.Errorf("starting writer database: %w", err)
	}
	if err := db.readerSQL.Run(ctx); err != nil {
		_ = db.writerSQL.Stop(ctx)
		return fmt.Errorf("starting reader database: %w", err)
	}
	if err := db.checkSchemaVersions(ctx); err != nil {
		_ = db.writerSQL.Stop(ctx)
		_ = db.readerSQL.Stop(ctx)
		return err
	}
	for _, r := range db.registry {
		r.build(db.writerSQL, db.readerSQL)
	}
	db.started.Store(true)
	return nil
}

// checkSchemaVersions refuses to start when the writer and reader are on different migration
// versions: the read model would silently diverge from the models this binary was built with.
// Databases without migration history (no goose table) are not compared.
func (db *DatabaseService) checkSchemaVersions(ctx context.Context) error {
	version := func(s sql.SQLServices) (int64, bool, error) {
		var exists bool
		if err := s.Client().NewRaw(`SELECT to_regclass('goose_db_version') IS NOT NULL`).Scan(ctx, &exists); err != nil || !exists {
			return 0, false, err
		}
		var v int64
		err := s.Client().NewRaw(`SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version WHERE is_applied`).Scan(ctx, &v)
		return v, true, err
	}
	w, wok, err := version(db.writerSQL)
	if err != nil {
		return fmt.Errorf("reading writer schema version: %w", err)
	}
	r, rok, err := version(db.readerSQL)
	if err != nil {
		return fmt.Errorf("reading reader schema version: %w", err)
	}
	// A side with no migration history counts as version 0, so an unmigrated reader next to a
	// migrated writer is refused too.
	if (wok || rok) && w != r {
		return fmt.Errorf("%w: writer is at version %d, reader at %d", ErrSchemaMismatch, w, r)
	}
	return nil
}

func (db *DatabaseService) Run(ctx context.Context) {
	if !db.started.Load() || db.cancelRunners != nil {
		return // not started, or the runners are already running (a second set would never be stopped)
	}
	// Runners live until Stop, not until ctx ends: fx's OnStart context, for one, is cancelled as soon
	// as start-up completes. ctx still carries its values (trace ids, tenant) to the runners.
	ctx, db.cancelRunners = context.WithCancel(context.WithoutCancel(ctx))
	for _, r := range db.registry {
		db.runners.Go(func() {
			if err := r.run(ctx); err != nil && ctx.Err() == nil && db.writerLogger != nil {
				db.writerLogger.Emit("database.run", func(l logger.LoggerLevel) {
					l.Error(err, "model runner stopped", "model", r.name)
				})
			}
		})
	}
}

func (db *DatabaseService) Stop(ctx context.Context) error {
	// Order matters: stop consuming changes, let the change hooks they queued finish, then close the
	// pools (database/sql waits for statements already running on the server).
	if db.cancelRunners != nil {
		db.cancelRunners()
		db.runners.Wait()
		db.cancelRunners = nil
	}
	db.started.Store(false)
	var errs []error
	for _, r := range db.registry {
		if r.shutdown != nil {
			errs = append(errs, r.shutdown(ctx))
			r.shutdown = nil
		}
	}
	if db.writerSQL != nil {
		errs = append(errs, db.writerSQL.Stop(ctx))
	}
	if db.readerSQL != nil {
		errs = append(errs, db.readerSQL.Stop(ctx))
	}
	return errors.Join(errs...)
}

type connPair struct{ writer, reader sql.SQLServices }

func (db *DatabaseService) Writer() sql.SQLServices {
	if p := db.conns.Load(); p != nil {
		return p.writer
	}
	return nil
}

func (db *DatabaseService) Reader() sql.SQLServices {
	if p := db.conns.Load(); p != nil {
		return p.reader
	}
	return nil
}
