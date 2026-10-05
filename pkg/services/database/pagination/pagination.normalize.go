package pagination

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
)

func (c *PaginationService[TData, TID]) normalizeFilters(
	ctx context.Context, filters []Filter,
) []Filter {
	if len(filters) == 0 {
		return filters
	}
	normalized := make([]Filter, 0, len(filters))
	for _, f := range filters {
		if f.Mode == ModeSearch && f.Field == "" {
			normalized = append(normalized, f)
			continue
		}
		if f.Mode == ModeCustom {
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
