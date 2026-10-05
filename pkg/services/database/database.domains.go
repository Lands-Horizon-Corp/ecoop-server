package database

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

	LogService interface {
		Log(ctx context.Context, message string)
		Error(ctx context.Context, message string)
		Warn(ctx context.Context, message string)
		Fatal(ctx context.Context, message string)
		Success(ctx context.Context, message string)
	}

	BroadcastService interface {
		Broadcast(channels []Channel, events Events, payload any) error
	}

	MessageBrokerService interface {
		Publish(ctx context.Context, topic string, key, value []byte) error
		Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error
	}

	CacheService interface {
		Get(ctx context.Context, key string) ([]byte, error)
		Set(ctx context.Context, key string, value any, ttl time.Duration) error
	}

	SQLService interface {
		Ping(ctx context.Context) error
		Client() *bun.DB
	}

	PaginationService[TData any, TID comparable] interface {
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

	CQRSServices[TData any, TResponse any, TRequest any, TID comparable] interface {
		// Lifecycle
		Run(ctx context.Context) error

		// Transactions
		Start(ctx context.Context) (bun.Tx, error)
		End(ctx context.Context, tx bun.Tx, err error) error

		// Events
		OnCreated(ctx context.Context, data *TData)
		OnUpdated(ctx context.Context, data *TData)
		OnDeleted(ctx context.Context, data *TData)

		// Model conversion
		ToModel(data *TData) *TResponse
		ToModels(data []*TData) []*TResponse

		// Create
		Create(ctx context.Context, data TData, preload ...string) (*TData, error)
		CreateFormat(ctx context.Context, data TData, preload ...string) (*TResponse, error)
		CreateMany(ctx context.Context, data []TData, preload ...string) ([]*TData, error)
		CreateManyFormat(ctx context.Context, data []TData, preload ...string) ([]*TResponse, error)
		CreateWithTx(ctx context.Context, tx bun.Tx, data TData, preload ...string) (*TData, error)
		CreateWithTxFormat(ctx context.Context, tx bun.Tx, data TData, preload ...string) (*TResponse, error)
		CreateManyWithTx(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TData, error)
		CreateManyWithTxFormat(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TResponse, error)

		// Create (validates TRequest, converts via FromRequest)
		CreateWithValidation(ctx context.Context, request TRequest, preload ...string) (*TData, error)
		CreateWithValidationFormat(ctx context.Context, request TRequest, preload ...string) (*TResponse, error)
		CreateManyWithValidation(ctx context.Context, requests []TRequest, preload ...string) ([]*TData, error)
		CreateManyWithValidationFormat(ctx context.Context, requests []TRequest, preload ...string) ([]*TResponse, error)
		CreateWithValidationTx(ctx context.Context, tx bun.Tx, request TRequest, preload ...string) (*TData, error)
		CreateWithValidationTxFormat(ctx context.Context, tx bun.Tx, request TRequest, preload ...string) (*TResponse, error)
		CreateManyWithValidationTx(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TData, error)
		CreateManyWithValidationTxFormat(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TResponse, error)

		// Update
		UpdateByID(ctx context.Context, id TID, data TData, preload ...string) (*TData, error)
		UpdateByIDFormat(ctx context.Context, id TID, data TData, preload ...string) (*TResponse, error)
		UpdateByIDWithTx(ctx context.Context, tx bun.Tx, id TID, data TData, preload ...string) (*TData, error)
		UpdateByIDWithTxFormat(ctx context.Context, tx bun.Tx, id TID, data TData, preload ...string) (*TResponse, error)
		UpdateMany(ctx context.Context, data []TData, preload ...string) ([]*TData, error)
		UpdateManyFormat(ctx context.Context, data []TData, preload ...string) ([]*TResponse, error)
		UpdateManyWithTx(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TData, error)
		UpdateManyWithTxFormat(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TResponse, error)

		// Update (validates TRequest, converts via FromRequest)
		UpdateByIDWithValidation(ctx context.Context, id TID, request TRequest, preload ...string) (*TData, error)
		UpdateByIDWithValidationFormat(ctx context.Context, id TID, request TRequest, preload ...string) (*TResponse, error)
		UpdateByIDWithValidationTx(ctx context.Context, tx bun.Tx, id TID, request TRequest, preload ...string) (*TData, error)
		UpdateByIDWithValidationTxFormat(ctx context.Context, tx bun.Tx, id TID, request TRequest, preload ...string) (*TResponse, error)
		UpdateManyWithValidation(ctx context.Context, requests []TRequest, preload ...string) ([]*TData, error)
		UpdateManyWithValidationFormat(ctx context.Context, requests []TRequest, preload ...string) ([]*TResponse, error)
		UpdateManyWithValidationTx(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TData, error)
		UpdateManyWithValidationTxFormat(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TResponse, error)

		// Increment
		IncrementByID(ctx context.Context, id TID, field string, delta float64) (*TData, error)
		IncrementByIDWithTx(ctx context.Context, tx bun.Tx, id TID, field string, delta float64) (*TData, error)

		// Delete
		DeleteByID(ctx context.Context, id TID) error
		DeleteByIDWithTx(ctx context.Context, tx bun.Tx, id TID) error
		DeleteMany(ctx context.Context, ids []TID) error
		DeleteManyWithTx(ctx context.Context, tx bun.Tx, ids []TID) error

		// Get by ID
		GetByID(ctx context.Context, id TID, preloads ...string) (*TData, error)
		GetByIDFormat(ctx context.Context, id TID, preloads ...string) (*TResponse, error)
		GetByIDWithTx(ctx context.Context, tx *bun.Tx, id TID, preloads ...string) (*TData, error)
		GetByIDWithTxFormat(ctx context.Context, tx *bun.Tx, id TID, preloads ...string) (*TResponse, error)

		// Find
		Find(ctx context.Context, filter StructuredFilter, preloads ...string) ([]*TData, error)
		FindFormat(ctx context.Context, filter StructuredFilter, preloads ...string) ([]*TResponse, error)
		FindWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter, preloads ...string) ([]*TData, error)
		FindWithTxFormat(ctx context.Context, tx *bun.Tx, filter StructuredFilter, preloads ...string) ([]*TResponse, error)

		// FindOne
		FindOne(ctx context.Context, filter StructuredFilter, preloads ...string) (*TData, error)
		FindOneFormat(ctx context.Context, filter StructuredFilter, preloads ...string) (*TResponse, error)
		FindOneWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter, preloads ...string) (*TData, error)
		FindOneWithTxFormat(ctx context.Context, tx *bun.Tx, filter StructuredFilter, preloads ...string) (*TResponse, error)

		// Count / Exists
		Count(ctx context.Context, filter StructuredFilter) (int64, error)
		CountWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) (int64, error)
		Exists(ctx context.Context, filter StructuredFilter) (bool, error)
		ExistsWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) (bool, error)

		// Max / Min
		Max(ctx context.Context, field string, filter StructuredFilter, preloads ...string) (*TData, error)
		MaxFormat(ctx context.Context, field string, filter StructuredFilter, preloads ...string) (*TResponse, error)
		MaxWithTx(ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string) (*TData, error)
		MaxWithTxFormat(ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string) (*TResponse, error)
		Min(ctx context.Context, field string, filter StructuredFilter, preloads ...string) (*TData, error)
		MinFormat(ctx context.Context, field string, filter StructuredFilter, preloads ...string) (*TResponse, error)
		MinWithTx(ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string) (*TData, error)
		MinWithTxFormat(ctx context.Context, tx *bun.Tx, field string, filter StructuredFilter, preloads ...string) (*TResponse, error)

		// Filter / Pagination
		Filter(ctx context.Context, filter StructuredFilter) ([]*TData, error)
		FilterFormat(ctx context.Context, filter StructuredFilter) ([]*TResponse, error)
		FilterWithTx(ctx context.Context, tx *bun.Tx, filter StructuredFilter) ([]*TData, error)
		FilterWithTxFormat(ctx context.Context, tx *bun.Tx, filter StructuredFilter) ([]*TResponse, error)
		Paginate(ctx context.Context, pagination Pagination) (PaginationResult[TData], error)
		PaginateFormat(ctx context.Context, pagination Pagination) (PaginationResult[TResponse], error)
		PaginateFilter(ctx context.Context, filter StructuredFilter, pagination Pagination) (PaginationResult[TData], error)
		PaginateFilterFormat(ctx context.Context, filter StructuredFilter, pagination Pagination) (PaginationResult[TResponse], error)
		PaginateWithHertz(ctx context.Context, tx *bun.Tx, filter StructuredFilter, reqCtx *app.RequestContext) (PaginationResult[TData], error)
		PaginateWithHertzFormat(ctx context.Context, tx *bun.Tx, filter StructuredFilter, reqCtx *app.RequestContext) (PaginationResult[TResponse], error)
	}
)
