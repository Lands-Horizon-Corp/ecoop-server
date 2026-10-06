package pagination

import (
	"context"

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
				c.warn(ctx, "filter dropped: custom filter has no function", "field", f.Field)
				continue
			}
			normalized = append(normalized, f)
			continue
		}
		f.Field = utils.NormalizeColumnName(f.Field)
		if utils.BunColumnFieldIndex[TData](f.Field) == -1 {
			c.warn(ctx, "filter dropped: field is not a column of the entity", "field", f.Field, "mode", string(f.Mode))
			continue
		}
		normalized = append(normalized, f)
	}
	return normalized
}
