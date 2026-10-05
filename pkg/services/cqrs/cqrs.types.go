package cqrs

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

type (
	Mode       string
	DataType   string
	Logic      string
	SortOrder  string
	Channel    string
	Events     []string
	ChangeType int

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

	CQRSQueuePayload[TData any] struct {
		EventID    string     `json:"event_id"`
		ChangeType ChangeType `json:"change_type"`
		Payload    TData      `json:"payload"`
	}

	ProcessedEvent struct {
		bun.BaseModel `bun:"table:processed_events,alias:pe"`
		EventID       string    `bun:"event_id,pk"`
		Channel       string    `bun:"channel,notnull"`
		CreatedAt     time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	}
)

type LogService interface {
	Log(ctx context.Context, message string)
	Error(ctx context.Context, message string)
	Warn(ctx context.Context, message string)
	Fatal(ctx context.Context, message string)
	Success(ctx context.Context, message string)
}

type BroadcastService interface {
	Broadcast(channels []Channel, events Events, payload any) error
}
type MessageBrokerService interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
	Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error
}

type CacheService interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
}

type SQLService interface {
	Ping(ctx context.Context) error
	Client() *bun.DB
}
type PaginationService[TData any, TID comparable] interface {
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
