package pagination

import (
	"context"
	"fmt"
	"strings"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/cloudwego/hertz/pkg/app"
)

func (c *PaginationService[TData, TID]) checkReady() error {
	if c.ReadSQLService == nil {
		return ErrReadServiceRequired
	}
	if c.ReadSQLService.Client() == nil {
		return ErrReadDBNotInitialized
	}
	if c.ColumnDefaultID == "" {
		return ErrColumnDefaultIDMissing
	}
	return nil
}

func parseFilter(ctx *app.RequestContext) (StructuredFilter, error) {
	filterParam := ctx.Query("filter")
	if filterParam == "" {
		return StructuredFilter{Logic: LogicAnd}, nil
	}
	filter, err := utils.DecodeQueryParam[StructuredFilter](filterParam)
	if err != nil {
		return StructuredFilter{}, fmt.Errorf("decoding filter: %w", err)
	}
	if filter.Logic == "" {
		filter.Logic = LogicAnd
	}
	return filter, nil
}

func parseSort(ctx *app.RequestContext) ([]SortField, error) {
	sortParam := ctx.Query("sort")
	if sortParam == "" {
		return nil, nil
	}
	sortFields, err := utils.DecodeQueryParam[[]SortField](sortParam)
	if err != nil {
		return nil, fmt.Errorf("decoding sort: %w", err)
	}
	for i, field := range sortFields {
		order := strings.ToLower(strings.TrimSpace(string(field.Order)))
		if order != string(SortOrderAsc) && order != string(SortOrderDesc) {
			sortFields[i].Order = SortOrderAsc
		} else {
			sortFields[i].Order = SortOrder(order)
		}
	}
	return sortFields, nil
}

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
				// c.warn(ctx, "filter dropped: custom filter has no function", "field", f.Field)
				continue
			}
			normalized = append(normalized, f)
			continue
		}
		f.Field = utils.NormalizeColumnName(f.Field)
		if utils.StructTagFieldIndex[TData]("bun", f.Field) == -1 {
			// c.warn(ctx, "filter dropped: field is not a column of the entity", "field", f.Field, "mode", string(f.Mode))
			continue
		}
		normalized = append(normalized, f)
	}
	return normalized
}
