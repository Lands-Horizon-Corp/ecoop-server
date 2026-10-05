package pagination

import (
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
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
