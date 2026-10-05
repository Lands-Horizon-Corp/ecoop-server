package pagination

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) EnableSearchIndex(
	ctx context.Context, fields ...string,
) error {
	if c.ReadSQLService == nil {
		return ErrReadServiceRequired
	}
	for _, field := range fields {
		if utils.BunColumnFieldIndex[TData](field) == -1 {
			return fmt.Errorf("%w: search index column %q", ErrUnknownField, field)
		}
	}

	db := c.ReadSQLService.Client()
	table := db.Table(reflect.TypeFor[TData]())
	columnArgs := make([]any, 0, len(fields)+1)
	columnArgs = append(columnArgs, bun.Ident(c.ColumnDefaultID))
	for _, field := range fields {
		columnArgs = append(columnArgs, bun.Ident(field))
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(columnArgs)), ", ")

	indexName := table.Name + "_search_idx"
	args := make([]any, 0, len(columnArgs)+3)
	args = append(args, bun.Ident(indexName), bun.Ident(table.Name))
	args = append(args, columnArgs...)
	args = append(args, c.ColumnDefaultID)

	query := fmt.Sprintf("CREATE INDEX IF NOT EXISTS ? ON ? USING bm25 (%s) WITH (key_field = ?)", placeholders)
	if _, err := db.NewRaw(query, args...).Exec(ctx); err != nil {
		return fmt.Errorf("creating search index: %w", err)
	}
	return nil
}
