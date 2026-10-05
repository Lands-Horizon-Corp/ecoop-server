package pagination

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/uptrace/bun"
)

func (c *PaginationService[TData, TID]) paginateMixedDirection(
	ctx context.Context,
	db bun.IDB,
	data *[]TData,
	extraFilter StructuredFilter,
	filterRoot StructuredFilter,
	sortFields []SortField,
	payload cursorPayload,
	backward bool,
	limit int64,
) error {
	orderFields := sortFields
	if backward {
		orderFields = reverseSortFields(sortFields)
	}
	applyOrder := func(q *bun.SelectQuery) *bun.SelectQuery {
		for _, sf := range orderFields {
			dir := "DESC"
			if sf.Order == SortOrderAsc {
				dir = "ASC"
			}
			q = q.OrderExpr("? "+dir+" NULLS LAST", bun.Ident(sf.Field))
		}
		return q
	}

	outer := db.NewSelect().Model(data)
	branchArgs := make([]any, 0, len(sortFields))
	for idx := range sortFields {
		branch := db.NewSelect().Model((*TData)(nil))
		branch, err := c.applyFilters(branch, extraFilter)
		if err != nil {
			return fmt.Errorf("applying hardcoded filter: %w", err)
		}
		branch, err = c.applyFilters(branch, filterRoot)
		if err != nil {
			return fmt.Errorf("applying filters: %w", err)
		}
		branch = appendCursorTerm(branch, sortFields, payload.Values, payload.Null, idx, backward)
		branch = applyOrder(branch).Limit(limit)

		name := fmt.Sprintf("cqrs_branch_%d", idx)
		outer = outer.With(name, branch)
		branchArgs = append(branchArgs, bun.Ident(name))
	}
	table := c.ReadSQLService.Client().Table(reflect.TypeFor[TData]())
	var fromExpr strings.Builder
	fromExpr.WriteString("(")
	for i := range branchArgs {
		if i > 0 {
			fromExpr.WriteString(" UNION ALL ")
		}
		fromExpr.WriteString("SELECT * FROM ?")
	}
	fromExpr.WriteString(") AS ?")
	branchArgs = append(branchArgs, bun.Ident(table.Alias))
	outer = outer.ModelTableExpr(fromExpr.String(), branchArgs...)

	outer = applyOrder(outer).Limit(limit)
	if err := outer.Scan(ctx); err != nil {
		return fmt.Errorf("scanning page: %w", err)
	}
	return nil
}
