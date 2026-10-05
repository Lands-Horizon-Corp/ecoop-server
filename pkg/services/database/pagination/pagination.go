package pagination

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

type PaginationService[TData any, TID comparable] struct {
	ReadSQLService    database.SQLService
	WriteSQLService   database.SQLService
	LogService        database.LogService
	ColumnDefaultID   string
	ColumnDefaultSort string
	Preloads          []string
}

func NewPaginationService[TData any, TID comparable](
	p PaginationService[TData, TID],
) database.PaginationServices[TData, TID] {
	if p.ColumnDefaultID == "" {
		p.ColumnDefaultID = "id"
	}
	if p.ColumnDefaultSort == "" {
		p.ColumnDefaultSort = "updated_at DESC"
	}
	if p.ReadSQLService == nil {
		p.ReadSQLService = p.WriteSQLService
	}
	if p.ReadSQLService == nil {
		panic("ReadSQLService or WriteSQLService must be initialized")
	}
	return &PaginationService[TData, TID]{
		ReadSQLService:    p.ReadSQLService,
		WriteSQLService:   p.WriteSQLService,
		LogService:        p.LogService,
		ColumnDefaultID:   p.ColumnDefaultID,
		ColumnDefaultSort: p.ColumnDefaultSort,
		Preloads:          p.Preloads,
	}
}

func (s *PaginationService[TData, TID]) Paginate(ctx context.Context, pagination database.Pagination) (database.PaginationResult[TData], error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) PaginateFilter(ctx context.Context, filter database.StructuredFilter, pagination database.Pagination) (database.PaginationResult[TData], error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) Filter(ctx context.Context, filter database.StructuredFilter) ([]*TData, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) FilterWithTx(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter) ([]*TData, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) PaginateWithHertz(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, reqCtx *app.RequestContext) (database.PaginationResult[TData], error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) Count(ctx context.Context, filter database.StructuredFilter) (int64, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) CountWithTx(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter) (int64, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) Exists(ctx context.Context, filter database.StructuredFilter) (bool, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) ExistsWithTx(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter) (bool, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) Find(ctx context.Context, filter database.StructuredFilter, preloads ...string) ([]*TData, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) FindWithTx(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string) ([]*TData, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) FindOne(ctx context.Context, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) FindOneWithTx(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) GetMax(ctx context.Context, field string, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) GetMin(ctx context.Context, field string, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) GetMaxWithTx(ctx context.Context, tx *bun.Tx, field string, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("implement me")
}

func (s *PaginationService[TData, TID]) GetMinWithTx(ctx context.Context, tx *bun.Tx, field string, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("implement me")
}
