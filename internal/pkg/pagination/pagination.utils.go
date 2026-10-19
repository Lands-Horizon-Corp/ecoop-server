package pagination

import (
	"fmt"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
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

func (c *PaginationService[TData, TID]) normalizeFilters(filters []Filter) []Filter {
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
				// A custom filter with no function has nothing to apply.
				continue
			}
			normalized = append(normalized, f)
			continue
		}
		f.Field = utils.NormalizeColumnName(f.Field)
		if utils.StructTagFieldIndex[TData]("bun", f.Field) == -1 {
			// A field that is not a column of the entity is dropped, not queried.
			continue
		}
		normalized = append(normalized, f)
	}
	return normalized
}

func (p *Pagination) Parse(ctx *app.RequestContext) error {
	if err := ctx.BindAndValidate(p); err != nil {
		return fmt.Errorf("invalid pagination parameters: %w", err)
	}
	filter, err := parseFilter(ctx)
	if err != nil {
		return err
	}
	p.Filter = filter
	sortFields, err := parseSort(ctx)
	if err != nil {
		return err
	}
	if sortFields != nil {
		p.Filter.SortFields = sortFields
	}
	return nil
}
