package pagination

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

type PaginationService[TData any, TID comparable] struct {
	ReadSQLService    sql.ISQLService
	WriteSQLService   sql.ISQLService
	ColumnDefaultID   string
	ColumnDefaultSort string
	Preloads          []string
}

func NewPaginationService[TData any, TID comparable](
	p *PaginationService[TData, TID],
) IPaginationService[TData, TID] {
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
		ColumnDefaultID:   p.ColumnDefaultID,
		ColumnDefaultSort: p.ColumnDefaultSort,
		Preloads:          p.Preloads,
	}
}

// Paginate implements IPaginationService.
func (s *PaginationService[TData, TID]) Paginate(ctx context.Context, pagination Pagination) (PaginationResult[TData], error) {
	panic("not implemented")
}

// PaginateFilter implements IPaginationService.
func (s *PaginationService[TData, TID]) PaginateFilter(ctx context.Context, filter StructuredFilter, pagination Pagination) (PaginationResult[TData], error) {
	panic("not implemented")
}

// Filter implements IPaginationService.
func (s *PaginationService[TData, TID]) Filter(ctx context.Context, filter StructuredFilter) ([]*TData, error) {
	panic("not implemented")
}

// FilterWithTx implements IPaginationService.
func (s *PaginationService[TData, TID]) FilterWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) ([]*TData, error) {
	panic("not implemented")
}

// PaginateWithHertz implements IPaginationService.
func (s *PaginationService[TData, TID]) PaginateWithHertz(ctx context.Context, tx *bun.Tx, filter StructuredFilter, reqCtx *app.RequestContext) (PaginationResult[TData], error) {
	panic("not implemented")
}

// CountWithTx implements IPaginationService.
func (s *PaginationService[TData, TID]) CountWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) (int64, error) {
	panic("not implemented")
}

// Exists implements IPaginationService.
func (s *PaginationService[TData, TID]) Exists(ctx context.Context, filter StructuredFilter) (bool, error) {
	panic("not implemented")
}

// ExistsWithTx implements IPaginationService.
func (s *PaginationService[TData, TID]) ExistsWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) (bool, error) {
	panic("not implemented")
}

// Find implements IPaginationService.
func (s *PaginationService[TData, TID]) Find(ctx context.Context, filter StructuredFilter, preloads ...string) ([]*TData, error) {
	panic("not implemented")
}

// FindWithTx implements IPaginationService.
func (s *PaginationService[TData, TID]) FindWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter, preloads ...string) ([]*TData, error) {
	panic("not implemented")
}

// FindOne implements IPaginationService.
func (s *PaginationService[TData, TID]) FindOne(ctx context.Context, filter StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

// FindOneWithTx implements IPaginationService.
func (s *PaginationService[TData, TID]) FindOneWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

// GetMax implements IPaginationService.
func (s *PaginationService[TData, TID]) GetMax(ctx context.Context, field string, filter StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

// GetMin implements IPaginationService.
func (s *PaginationService[TData, TID]) GetMin(ctx context.Context, field string, filter StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

// GetMaxWithTx implements IPaginationService.
func (s *PaginationService[TData, TID]) GetMaxWithTx(ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

// GetMinWithTx implements IPaginationService.
func (s *PaginationService[TData, TID]) GetMinWithTx(ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}
