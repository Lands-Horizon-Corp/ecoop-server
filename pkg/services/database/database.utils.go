package database

import (
	"fmt"
	"strings"

	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/cloudwego/hertz/pkg/app"
)

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
