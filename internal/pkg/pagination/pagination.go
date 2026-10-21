package pagination

import (
	"context"
	"log/slog"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type PaginationService[TData any, TID comparable] struct {
	ReadSQLService    sql.ISQLService
	WriteSQLService   sql.ISQLService
	ColumnDefaultID   string
	ColumnDefaultSort string
	Preloads          []string
	Observer          observability.IObserver

	gate  *utils.Gate
	obs   observability.IObserver
	table string
}

func (c *PaginationService[TData, TID]) begin(ctx context.Context, name string) (context.Context, observability.Operation) {
	if c.obs == nil {
		return observability.Nop().Begin(ctx, observability.ComponentPagination, name)
	}
	return c.obs.Begin(ctx, observability.ComponentPagination, name, slog.String(observability.KeyTable, c.table))
}

func NewPaginationService[TData any, TID comparable](
	p *PaginationService[TData, TID], inflight *utils.Inflight,
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
		Observer:          p.Observer,
		gate:              utils.NewGate(inflight),
		obs:               observability.Pick(p.Observer),
		table:             utils.BunTable[TData]().Name,
	}
}
