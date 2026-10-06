package database

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/go-playground/validator/v10"
)

type DatabaseService struct {
	started  bool
	registry map[reflect.Type]*registration

	// cancelRunners and runners let Stop end the model runners before it
	// closes the connections they use.
	cancelRunners context.CancelFunc
	runners       sync.WaitGroup

	readerSQL sql.SQLServices
	writerSQL sql.SQLServices

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
) *DatabaseService {
	return &DatabaseService{
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
}

func (db *DatabaseService) Start(ctx context.Context) error {
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
	)
	if err := db.writerSQL.Run(ctx); err != nil {
		return fmt.Errorf("starting writer database: %w", err)
	}
	if err := db.readerSQL.Run(ctx); err != nil {
		_ = db.writerSQL.Stop(ctx)
		return fmt.Errorf("starting reader database: %w", err)
	}
	for _, r := range db.registry {
		r.build(db.writerSQL, db.readerSQL)
	}
	db.started = true
	return nil
}

func (db *DatabaseService) Run(ctx context.Context) {
	if !db.started {
		return
	}
	ctx, db.cancelRunners = context.WithCancel(ctx)
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
	if db.cancelRunners != nil {
		db.cancelRunners()
		db.runners.Wait()
		db.cancelRunners = nil
	}
	if db.writerSQL != nil {
		return db.writerSQL.Stop(ctx)
	}
	if db.readerSQL != nil {
		return db.readerSQL.Stop(ctx)
	}
	db.started = false
	return nil
}

func (db *DatabaseService) Writer() sql.SQLServices {
	return db.writerSQL
}

func (db *DatabaseService) Reader() sql.SQLServices {
	return db.readerSQL
}
