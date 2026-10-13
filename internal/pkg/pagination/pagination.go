package pagination

import (
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type PaginationService[TData any, TID comparable] struct {
	ReadSQLService    sql.ISQLService
	WriteSQLService   sql.ISQLService
	ColumnDefaultID   string
	ColumnDefaultSort string
	Preloads          []string

	gate *utils.Gate
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
		gate:              utils.NewGate(inflight),
	}
}
