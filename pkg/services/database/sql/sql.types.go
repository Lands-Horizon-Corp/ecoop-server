package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type SQLService struct {
	dsn         string
	maxIdleConn int
	maxOpenConn int
	db          *bun.DB
}

func NewSQLService(
	dsn string,
	maxIdleConn int,
	maxOpenConn int,
) SQLServices {
	return &SQLService{
		dsn:         dsn,
		maxIdleConn: maxIdleConn,
		maxOpenConn: maxOpenConn,
	}
}

func (s *SQLService) Client() *bun.DB {
	return s.db
}

func (s *SQLService) Ping(ctx context.Context) error {
	if s.db == nil {
		return fmt.Errorf("database connection is not initialized")
	}
	return s.db.PingContext(ctx)
}

func (s *SQLService) Run(ctx context.Context) error {
	sqldb, err := sql.Open("postgres", s.dsn)
	if err != nil {
		return fmt.Errorf("failed to open sql connection: %w", err)
	}
	sqldb.SetMaxIdleConns(s.maxIdleConn)
	sqldb.SetMaxOpenConns(s.maxOpenConn)
	goose.SetDialect("postgres")
	if err := goose.Up(sqldb, "./migrations"); err != nil {
		return fmt.Errorf("failed to run goose migrations: %w", err)
	}
	s.db = bun.NewDB(sqldb, pgdialect.New())
	if err := s.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}
	return nil
}

func (s *SQLService) Stop(ctx context.Context) error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
