package pagination

import (
	"context"
	"errors"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"reflect"
	"strings"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/jackc/pgx/v5/pgconn"
)

func (c *PaginationService[TData, TID]) fields(kv []any) []any {
	return append([]any{"component", "pagination", "entity", reflect.TypeFor[TData]().String()}, kv...)
}

func (c *PaginationService[TData, TID]) emit(name string, write func(logger.LoggerLevel)) {
	if c.Log != nil {
		c.Log.Emit(name, write)
	}
}

func (c *PaginationService[TData, TID]) warn(_ context.Context, msg string, kv ...any) {
	c.emit("pagination.warn", func(l logger.LoggerLevel) {
		l.Warn(msg, c.fields(kv)...)
	})
}

func (c *PaginationService[TData, TID]) error(_ context.Context, err error, msg string, kv ...any) {
	c.emit("pagination.error", func(l logger.LoggerLevel) {
		l.Error(sql.Redact(err), msg, c.fields(kv)...)
	})
}

func (c *PaginationService[TData, TID]) report(
	ctx context.Context, op string, started time.Time, err error, extra []any, filters ...StructuredFilter,
) error {
	if err == nil || c.Log == nil {
		return err
	}
	kv := append([]any{"operation", op, "duration_ms", time.Since(started).Milliseconds()}, extra...)
	kv = append(kv, filterShape(filters)...)
	if reason := requestErrorReason(err); reason != "" {
		c.warn(ctx, "pagination request rejected", append(kv, "reason", reason)...)
		return err
	}
	c.error(ctx, err, "pagination query failed", kv...)
	return err
}

func requestErrorReason(err error) string {
	switch {
	case errors.Is(err, ErrUnknownField):
		return "unknown_field"
	case errors.Is(err, ErrInvalidFilter):
		return "invalid_filter"
	case errors.Is(err, ErrInvalidCursor):
		return "invalid_cursor"
	case errors.Is(err, ErrRowLockUnsupported):
		return "row_lock_unsupported"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22") {
		return "invalid_value"
	}
	return ""
}

func filterShape(filters []StructuredFilter) []any {
	var fs, sorts []string
	preloads, logic := 0, ""
	for _, f := range filters {
		for _, x := range f.Filters {
			fs = append(fs, x.Field+":"+string(x.Mode))
		}
		for _, s := range f.SortFields {
			sorts = append(sorts, s.Field+" "+string(s.Order))
		}
		preloads += len(f.Preload)
		if logic == "" {
			logic = string(f.Logic)
		}
	}
	return []any{
		"filters", fs, "sort", sorts, "preloads", preloads, "logic", logic}
}
