package database

import (
	"context"
	"io"
	"os"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
)

type DatabaseService struct {
	registry map[any]cqrs.CQRSServices[any, any, any, any]

	readerSQL sql.SQLServices
	writerSQL sql.SQLServices

	readerLogger logger.LogContextService
	writerLogger logger.LogContextService

	writerDsn string
	readerDsn string

	maxIdleConn int
	maxOpenConn int

	migrations  *os.File
	autoMigrate bool
	output      io.Writer
	models      []any
}

func NewDatabaseService(
	writerDsn string,
	readerDsn string,

	maxIdleConn int,
	maxOpenConn int,

	readerLogger logger.LogContextService,
	writerLogger logger.LogContextService,

	migrations *os.File,
	autoMigrate bool,
	output io.Writer,
	models []any,
) *DatabaseService {
	return &DatabaseService{
		registry:     make(map[any]cqrs.CQRSServices[any, any, any, any]),
		writerDsn:    writerDsn,
		readerDsn:    readerDsn,
		readerLogger: readerLogger,
		writerLogger: writerLogger,
		maxIdleConn:  maxIdleConn,
		maxOpenConn:  maxOpenConn,
		migrations:   migrations,
		autoMigrate:  autoMigrate,
		output:       output,
		models:       models,
	}
}

func (db *DatabaseService) Start(ctx context.Context) {
	if db.writerSQL != nil {
		db.writerSQL.Stop(ctx)
	}
	if db.readerSQL != nil {
		db.readerSQL.Stop(ctx)
	}
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
}

func (db *DatabaseService) Stop(ctx context.Context) {
	if db.writerSQL != nil {
		db.writerSQL.Stop(ctx)
	}
	if db.readerSQL != nil {
		db.readerSQL.Stop(ctx)
	}
}

func (db *DatabaseService) Register[TData any, TResponse any, TRequest any, TID comparable](
	model any) map[any]cqrs.CQRSServices[any, any, any, any] {
	return db.registry
}

func (db *DatabaseService) GetRegistry() map[any]cqrs.CQRSServices[any, any, any, any] {
	return db.registry
}
