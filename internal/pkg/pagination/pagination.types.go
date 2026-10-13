package pagination

import (
	"context"
	"errors"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
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
)

var (
	ErrNotInitialized       = errors.New("sql: database connection is not initialized")
	ErrNoMigrationsDir      = errors.New("sql: no migrations directory was provided")
	ErrInvalidMigrationsDir = errors.New("sql: migrations directory is not usable")
	ErrInvalidSteps         = errors.New("sql: steps must be greater than zero")
	ErrTooManySteps         = errors.New("sql: more steps requested than migrations available")
	ErrNoModels             = errors.New("sql: at least one model is required")
	ErrInvalidName          = errors.New("sql: migration name must contain a letter or digit")
	ErrPendingMigrations    = errors.New("sql: apply pending migrations before generating a new one")
	ErrInvalidMigration     = errors.New("sql: generated migration is not valid")

	ErrNilTx       = errors.New("sql: transaction is nil or was never started")
	ErrInvalidText = errors.New("sql: text is not valid UTF-8 or contains a NUL byte")
	ErrUnsafeText  = errors.New("sql: text contains a replacement, control or BiDi override character")

	ErrReadServiceRequired    = errors.New("pagination: ReadSQLService must be set")
	ErrReadDBNotInitialized   = errors.New("pagination: read db is not initialized")
	ErrColumnDefaultIDMissing = errors.New("pagination: ColumnDefaultID must be set")
	ErrInvalidPartitionKey    = errors.New("pagination: partition column must be part of the primary key")

	ErrUnknownField       = errors.New("pagination: unknown field")
	ErrInvalidFilter      = errors.New("pagination: invalid filter")
	ErrInvalidCursor      = errors.New("pagination: invalid cursor")
	ErrRowLockUnsupported = errors.New("pagination: row locking is not supported with mixed-direction cursor pagination")
)

type (
	Mode      string
	DataType  string
	Logic     string
	SortOrder string

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
		Field    string       `json:"field"`
		Value    any          `json:"value"`
		Mode     Mode         `json:"mode"`
		DataType DataType     `json:"dataType"`
		Custom   CustomFilter `json:"-"`
	}

	StructuredFilter struct {
		Filters    []Filter    `json:"filters"`
		SortFields []SortField `json:"sortFields"`
		Logic      Logic       `json:"logic"`
		Preload    []string    `json:"preload"`
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
