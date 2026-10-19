package pagination

import (
	"context"
	"errors"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

const (
	ModeEqual       Mode = "equal"
	ModeNotEqual    Mode = "notEqual"
	ModeContains    Mode = "contains"
	ModeNotContains Mode = "notContains"
	ModeStartsWith  Mode = "startsWith"
	ModeEndsWith    Mode = "endsWith"
	ModeSearch      Mode = "search"
	ModeCustom      Mode = "custom"
	ModeInside      Mode = "inside"
	ModeOutside     Mode = "outside"
	ModeGT          Mode = "gt"
	ModeGTE         Mode = "gte"
	ModeLT          Mode = "lt"
	ModeLTE         Mode = "lte"
	ModeRange       Mode = "range"
	ModeBefore      Mode = "before"
	ModeAfter       Mode = "after"
	ModeIsEmpty     Mode = "isEmpty"
	ModeIsNotEmpty  Mode = "isNotEmpty"

	DataTypeNumber DataType = "number"
	DataTypeText   DataType = "text"
	DataTypeBool   DataType = "bool"
	DataTypeDate   DataType = "date"
	DataTypeTime   DataType = "time"

	LogicAnd Logic = "and"
	LogicOr  Logic = "or"

	SortOrderAsc  SortOrder = "asc"
	SortOrderDesc SortOrder = "desc"

	kindInstant     colKind = iota // unknown or timestamp: compared as instants
	kindDate                       // date: a calendar day with no zone
	kindTimestampTZ                // timestamptz
	kindClock                      // time, timetz: a time of day with no date
)

var (
	ErrNilTx       = utils.ErrNilTx
	ErrInvalidText = utils.ErrInvalidText
	ErrUnsafeText  = utils.ErrUnsafeText

	ErrReadServiceRequired    = errors.New("pagination: ReadSQLService must be set")
	ErrReadDBNotInitialized   = errors.New("pagination: read db is not initialized")
	ErrColumnDefaultIDMissing = errors.New("pagination: ColumnDefaultID must be set")
	ErrInvalidPartitionKey    = errors.New("pagination: partition column must be part of the primary key")

	ErrUnknownField       = errors.New("pagination: unknown field")
	ErrInvalidFilter      = errors.New("pagination: invalid filter")
	ErrInvalidCursor      = errors.New("pagination: invalid cursor")
	ErrRowLockUnsupported = errors.New("pagination: row locking is not supported with mixed-direction cursor pagination")
	ErrNotDate            = utils.ErrNotDate
)

type (
	Mode      string
	DataType  string
	Logic     string
	SortOrder string
	colKind   int

	// clockValue is a parsed time of day: the clock as written, and the offset
	// it was written in, if it named one.
	clockValue struct {
		lit string
		off *int
	}
	DateValue struct {
		t       time.Time
		hasTime bool
	}

	RangeNumber struct {
		From float64 `json:"from"`
		To   float64 `json:"to"`
	}

	RangeDate struct {
		From time.Time `json:"from"`
		To   time.Time `json:"to"`
	}

	CustomFilter func(q *bun.SelectQuery, value any) (*bun.SelectQuery, error)

	SortField struct {
		Field string    `json:"field"`
		Order SortOrder `json:"order"`
	}

	Filter struct {
		Field    string   `json:"field"`
		Value    any      `json:"value"`
		Mode     Mode     `json:"mode"`
		DataType DataType `json:"dataType"`
		// Timezone is the business zone this filter's dates and times are read
		// in: an IANA name ("Asia/Manila") or an offset ("+08:00"). A bare date
		// then means that zone's whole day, a time without an offset is local
		// to it, and a time of day is compared in it. Empty uses the
		// StructuredFilter's zone, then UTC.
		Timezone string       `json:"timezone"`
		Custom   CustomFilter `json:"-"`
	}

	StructuredFilter struct {
		Filters    []Filter    `json:"filters"`
		SortFields []SortField `json:"sortFields"`
		Logic      Logic       `json:"logic"`
		Preload    []string    `json:"preload"`
		// Timezone is the default for every filter that does not set its own.
		Timezone string `json:"timezone"`
	}

	Pagination struct {
		Filter   StructuredFilter `json:"filter"`
		PageSize int              `query:"pageSize" default:"10"`
		Cursor   *string          `query:"cursor"`
	}

	PaginationResult[T any] struct {
		Data           []*T    `json:"data"`
		CurrentCursor  *string `json:"currentCursor"`
		NextCursor     *string `json:"nextCursor"`
		PreviousCursor *string `json:"previousCursor"`
		PageSize       int     `json:"pageSize"`
	}

	cursorPayload struct {
		Values   []string `json:"v"`
		Null     []bool   `json:"n,omitempty"`
		Backward bool     `json:"b,omitempty"`
	}
	IPaginationService[TData any, TID comparable] interface {
		Paginate(ctx context.Context, pagination Pagination) (PaginationResult[TData], error)
		PaginateFilter(ctx context.Context, filter StructuredFilter, pagination Pagination) (PaginationResult[TData], error)
		Filter(ctx context.Context, filter StructuredFilter) ([]*TData, error)
		FilterWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) ([]*TData, error)
		PaginateWithHertz(ctx context.Context, tx *bun.Tx, filter StructuredFilter, reqCtx *app.RequestContext) (PaginationResult[TData], error)
		Count(ctx context.Context, filter StructuredFilter) (int64, error)
		CountWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) (int64, error)
		Exists(ctx context.Context, filter StructuredFilter) (bool, error)
		ExistsWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) (bool, error)
		Find(ctx context.Context, filter StructuredFilter, preloads ...string) ([]*TData, error)
		FindWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter, preloads ...string) ([]*TData, error)
		FindOne(ctx context.Context, filter StructuredFilter, preloads ...string) (*TData, error)
		FindOneWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter, preloads ...string) (*TData, error)
		GetMax(ctx context.Context, field string, filter StructuredFilter, preloads ...string) (*TData, error)
		GetMin(ctx context.Context, field string, filter StructuredFilter, preloads ...string) (*TData, error)
		GetMaxWithTx(ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string) (*TData, error)
		GetMinWithTx(ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string) (*TData, error)
	}
)
