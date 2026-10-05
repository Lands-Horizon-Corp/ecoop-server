package pagination

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
)

func (c *PaginationService[TData, TID]) normalizeFilters(
	ctx context.Context, filters []database.Filter,
) []database.Filter {
	if len(filters) == 0 {
		return filters
	}
	normalized := make([]database.Filter, 0, len(filters))
	for _, f := range filters {
		if f.Mode == database.ModeSearch && f.Field == "" {
			normalized = append(normalized, f)
			continue
		}
		if f.Mode == database.ModeCustom {
			if f.Custom == nil {
				c.warn(ctx, fmt.Sprintf("pagination: dropping custom filter %q without a function", f.Field))
				continue
			}
			normalized = append(normalized, f)
			continue
		}
		f.Field = utils.NormalizeColumnName(f.Field)
		if utils.BunColumnFieldIndex[TData](f.Field) == -1 {
			c.warn(ctx, fmt.Sprintf("pagination: dropping filter for unknown field %q", f.Field))
			continue
		}
		normalized = append(normalized, f)
	}
	return normalized
}
