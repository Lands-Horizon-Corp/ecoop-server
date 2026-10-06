package pagination

import (
	"context"
	"fmt"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"math"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func (c *PaginationService[TData, TID]) Pagination(
	ctx context.Context,
	pagination Pagination,
	preloads ...string,
) (*PaginationResult[TData], error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	return c.paginate(ctx, c.ReadSQLService.Client(), StructuredFilter{}, pagination, false, preloads...)
}

// checkDB rejects a nil or zero-value connection or transaction before it is dereferenced.
func checkDB(db bun.IDB) error { return sql.CheckDB(db, ErrReadDBNotInitialized) }

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

func (c *PaginationService[TData, TID]) paginate(
	ctx context.Context,
	db bun.IDB,
	extraFilter StructuredFilter,
	pagination Pagination,
	forUpdate bool,
	preloads ...string,
) (*PaginationResult[TData], error) {
	if forUpdate && sql.RowLocksDisabled(ctx) {
		forUpdate = false
	}
	started := time.Now()
	result, err := sql.Scoped(ctx, db, func(q bun.IDB) (*PaginationResult[TData], error) {
		return c.paginateQuery(ctx, q, extraFilter, pagination, forUpdate, preloads...)
	})
	if err != nil {
		return nil, c.report(ctx, "paginate", started, err,
			[]any{"page_size", pagination.PageSize, "has_cursor", pagination.Cursor != nil, "for_update", forUpdate},
			extraFilter, pagination.Filter)
	}
	return result, nil
}

func (c *PaginationService[TData, TID]) paginateQuery(
	ctx context.Context,
	db bun.IDB,
	extraFilter StructuredFilter,
	pagination Pagination,
	forUpdate bool,
	preloads ...string,
) (*PaginationResult[TData], error) {
	if err := checkDB(db); err != nil {
		return nil, err
	}
	if pagination.PageSize <= 0 {
		pagination.PageSize = 30
	} else if pagination.PageSize > math.MaxInt-1 {
		pagination.PageSize = math.MaxInt - 1
	}

	pagination.Filter.Filters = c.normalizeFilters(ctx, pagination.Filter.Filters)
	// Find, FindWithTx and Filter pass their filter as extraFilter; honor its sort order.
	if len(pagination.Filter.SortFields) == 0 {
		pagination.Filter.SortFields = extraFilter.SortFields
	}

	sortFields, err := c.resolveSortFields(pagination.Filter.SortFields)
	if err != nil {
		return nil, fmt.Errorf("resolving sort fields: %w", err)
	}
	payload, hasCursor, err := c.decodeCursor(pagination.Cursor, sortFields)
	if err != nil {
		return nil, fmt.Errorf("applying cursor: %w", err)
	}
	backward := hasCursor && payload.Backward
	op, uniform := cursorIsUniform(sortFields, backward)
	if uniform && anyNullableSortField[TData](sortFields) {
		uniform = false
	}
	limit := int64(pagination.PageSize + 1)

	var data []TData
	if hasCursor && !uniform {
		if forUpdate {
			return nil, ErrRowLockUnsupported
		}
		if err := c.paginateMixedDirection(ctx, db, &data, extraFilter, pagination.Filter, sortFields, payload, backward, limit); err != nil {
			return nil, err
		}
	} else {
		q := db.NewSelect().Model(&data)
		if q, err = c.applyFilters(q, extraFilter); err != nil {
			return nil, fmt.Errorf("applying hardcoded filter: %w", err)
		}
		if q, err = c.applyFilters(q, pagination.Filter); err != nil {
			return nil, fmt.Errorf("applying filters: %w", err)
		}
		if hasCursor {
			q = applyCursorUniform(q, sortFields, payload.Values, op)
		}
		orderFields := sortFields
		if backward {
			orderFields = reverseSortFields(sortFields)
		}
		for _, sf := range orderFields {
			dir := "DESC"
			if sf.Order == SortOrderAsc {
				dir = "ASC"
			}
			q = q.OrderExpr("? "+dir+" NULLS LAST", bun.Ident(sf.Field))
		}
		q = q.Limit(limit)
		if forUpdate && db.Dialect().Name() == dialect.PG {
			q = q.For("UPDATE")
		}
		if err := q.Scan(ctx); err != nil {
			return nil, fmt.Errorf("scanning page: %w", err)
		}
	}

	hasMore := len(data) > pagination.PageSize
	if hasMore {
		data = data[:pagination.PageSize]
	}
	if backward {
		for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
			data[i], data[j] = data[j], data[i]
		}
	}

	droppedPreloads, err := utils.ApplyPreloadsMany(ctx, db, &data, c.Preloads, preloads...)
	for _, d := range droppedPreloads {
		c.warn(ctx, "preload dropped: unknown relation", "relation", d)
	}
	if err != nil {
		return nil, fmt.Errorf("loading preloads: %w", err)
	}

	result := &PaginationResult[TData]{
		PageSize:      pagination.PageSize,
		CurrentCursor: pagination.Cursor,
	}
	if len(data) > 0 {
		if !backward {
			if hasMore {
				next, err := c.encodeCursor(&data[len(data)-1], sortFields, false)
				if err != nil {
					return nil, fmt.Errorf("encoding next cursor: %w", err)
				}
				result.NextCursor = &next
			}
			if hasCursor {
				prev, err := c.encodeCursor(&data[0], sortFields, true)
				if err != nil {
					return nil, fmt.Errorf("encoding previous cursor: %w", err)
				}
				result.PreviousCursor = &prev
			}
		} else {
			next, err := c.encodeCursor(&data[len(data)-1], sortFields, false)
			if err != nil {
				return nil, fmt.Errorf("encoding next cursor: %w", err)
			}
			result.NextCursor = &next
			if hasMore {
				prev, err := c.encodeCursor(&data[0], sortFields, true)
				if err != nil {
					return nil, fmt.Errorf("encoding previous cursor: %w", err)
				}
				result.PreviousCursor = &prev
			}
		}
	}

	result.Data = make([]*TData, len(data))
	for i := range data {
		result.Data[i] = &data[i]
	}
	return result, nil
}
