package sql

import (
	"context"
	"fmt"
	"os"

	"github.com/uptrace/bun"
)

type SQLService struct {
	dsn         string
	maxIdleConn int
	maxOpenConn int
	db          *bun.DB
	file        *os.File
}

func NewSQLService(
	dsn string,
	maxIdleConn int,
	maxOpenConn int,
	file *os.File,
) SQLServices {
	return &SQLService{
		dsn:         dsn,
		maxIdleConn: maxIdleConn,
		maxOpenConn: maxOpenConn,
		file:        file,
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
