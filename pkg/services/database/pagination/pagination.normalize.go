package pagination

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
)

// normalizeFilters runs every filter's Field through
// utils.NormalizeColumnName (client input arrives in whatever casing/
// spacing the caller happened to send — "userName", " User Name ",
// "UserName" should all still resolve the same real column) and drops any
// filter whose normalized Field still doesn't match a real TData column.
//
// A dropped filter only warns (via LogService, if set) rather than failing
// the whole page: an unknown/stale/typo'd filter field is untrusted client
// input, not a caller bug worth a hard error — the safe behavior is to
// ignore that one term and keep serving the rest of the request, the same
// way an unknown query-string parameter is typically ignored rather than
// rejected outright.
func (c *PaginationService[TData, TID]) normalizeFilters(
	ctx context.Context, filters []database.Filter,
) []database.Filter {
	if len(filters) == 0 {
		return filters
	}
	normalized := make([]database.Filter, 0, len(filters))
	for _, f := range filters {
		// ModeSearch's empty-Field form means "search every column
		// EnableSearchIndex indexed" (see applyFilterTerm) — it's not a
		// real column name to normalize/validate at all, so it must pass
		// through untouched rather than getting normalized to "" and then
		// dropped as an unknown field.
		if f.Mode == database.ModeSearch && f.Field == "" {
			normalized = append(normalized, f)
			continue
		}
		// A client cannot set Custom (json:"-"), so a custom filter without one is not runnable.
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
