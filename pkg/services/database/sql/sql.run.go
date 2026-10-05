package sql

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func (s *SQLService) Run(ctx context.Context) error {
	sqldb, err := sql.Open("pgx", s.dsn)
	if err != nil {
		return fmt.Errorf("failed to open sql connection: %w", err)
	}
	sqldb.SetMaxIdleConns(s.maxIdleConn)
	sqldb.SetMaxOpenConns(s.maxOpenConn)

	s.sqldb = sqldb
	s.db = bun.NewDB(sqldb, pgdialect.New())
	if err := s.Ping(ctx); err != nil {
		_ = s.Stop(ctx)
		return fmt.Errorf("failed to ping database: %w", err)
	}
	if err := s.Migrate(ctx); err != nil {
		_ = s.Stop(ctx)
		return fmt.Errorf("failed to run goose migrations: %w", err)
	}
	return nil
}

func (s *SQLService) Stop(ctx context.Context) error {
	s.sqldb = nil
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
