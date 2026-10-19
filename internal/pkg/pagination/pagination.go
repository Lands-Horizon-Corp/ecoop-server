package pagination

import (
	"context"
	"log/slog"
	"reflect"

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
	// Observer receives the service's metrics and traces. Optional: nil
	// observes nothing.
	Observer observability.IObserver

	gate  *utils.Gate
	obs   observability.IObserver
	table string // the model's table, the only thing a metric is labelled with
}

// begin starts the Operation of one query, labelled with the model's table. A
// service built as a bare struct literal has no observer, so it falls back to
// Nop without writing. The filter values are never recorded.
func (c *PaginationService[TData, TID]) begin(ctx context.Context, name string) (context.Context, observability.Operation) {
	if c.obs == nil {
		return observability.Nop().Begin(ctx, observability.ComponentPagination, name)
	}
	return c.obs.Begin(ctx, observability.ComponentPagination, name, slog.String(observability.KeyTable, c.table))
}

// tableOf is the name bun gives TData's table, or "" when TData is not a struct.
func tableOf[TData any]() string {
	if t := utils.BunTable[TData](); t != nil {
		return t.Name
	}
	return reflect.TypeFor[TData]().Name()
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
		table:             tableOf[TData](),
	}
}
