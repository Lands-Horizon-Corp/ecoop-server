package sql

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
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
