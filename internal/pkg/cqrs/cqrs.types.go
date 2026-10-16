package cqrs

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

const (
	// CheckpointTable is the read-database table that records, per consumer
	// group, topic and partition, how far the change stream has been applied.
	CheckpointTable = "cqrs_checkpoints"

	// RowStateTable is the read-database table that remembers, per row, the WAL
	// position (LSN) of the last change applied to it, and whether that change was
	// a delete. Only a service with OrderByLSN uses it.
	RowStateTable = "cqrs_row_state"

	// unavailableValue is what Debezium puts in place of an unchanged TOAST
	// column it cannot read (see REPLICA IDENTITY FULL).
	unavailableValue = "__debezium_unavailable_value"

	defaultSchema       = "public"
	defaultBatchSize    = 100
	defaultApplyTimeout = 30 * time.Second
	defaultRetryBackoff = 100 * time.Millisecond
	defaultMaxBackoff   = 5 * time.Second
	defaultHookWorkers  = 16
	hookTimeout         = 10 * time.Second
	deadLetterTimeout   = 10 * time.Second
	upsertChunk         = 500

	// regressionSlack is how far a record already covered by the checkpoint may be
	// "from the future" of the last record applied before the offsets are taken to
	// have gone backwards. Producers stamp records when they create them, so a few
	// records can be out of order by milliseconds; minutes cannot be.
	regressionSlack = 5 * time.Minute

	noteTombstone = "tombstone"
	noteTruncate  = "truncate"
	noteOther     = "unsupported operation"
)

var (
	ErrFromRequestNotSet           = errors.New("cqrs: FromRequest must be set to use *WithValidation methods")
	ErrMessageBrokerNotInitialized = errors.New("cqrs: message broker service is not initialized")
	ErrReadDBNotInitialized        = errors.New("cqrs: read db is not initialized")
	ErrWriteDBNotInitialized       = errors.New("cqrs: write db is not initialized")
	ErrWriteDBUnreachable          = errors.New("cqrs: write db is not reachable")
	ErrReadDBUnreachable           = errors.New("cqrs: read db is not reachable")
	ErrUnknownField                = errors.New("cqrs: unknown field")
	ErrInvalidText                 = utils.ErrInvalidText
	ErrUnsafeText                  = utils.ErrUnsafeText
	ErrNilTx                       = utils.ErrNilTx

	ErrNotConstructed     = errors.New("cqrs: service is not set up; create it with NewCQRS")
	ErrNilContext         = errors.New("cqrs: nil context")
	ErrCDCNotInitialized  = errors.New("cqrs: cdc service is not initialized")
	ErrModelNotStruct     = errors.New("cqrs: the model must be a struct")
	ErrNoPrimaryKey       = errors.New("cqrs: no usable primary key")
	ErrReplicaIdentity    = errors.New("cqrs: the write table must have REPLICA IDENTITY FULL")
	ErrReadTableMissing   = errors.New("cqrs: the table, or some of its columns, does not exist in the read db")
	ErrWriteTableMissing  = errors.New("cqrs: the table does not exist in the write db")
	ErrMalformedChange    = errors.New("cqrs: malformed change event")
	ErrUnavailableValue   = errors.New("cqrs: column value unavailable (an unchanged TOAST column; set REPLICA IDENTITY FULL)")
	ErrStopped            = errors.New("cqrs: service is stopped")
	ErrConnectionNotReady = errors.New("cqrs: database connection is not ready")
	ErrUnsupportedColumn  = errors.New("cqrs: unsupported column type")
	ErrOffsetRegression   = errors.New("cqrs: the topic's offsets went back behind the checkpoint (the topic was probably recreated); " +
		"replication is paused so no change is skipped; delete this topic's rows from cqrs_checkpoints to resume from the start of the topic")
	ErrBadBatch = errors.New("cqrs: malformed batch")

	timeType = reflect.TypeFor[time.Time]()
)

// Checkpoint records how far one partition of a topic has been applied to the
// read database. It is written in the same transaction as the rows it covers,
// so the rows and the position never disagree, whatever crashes or restarts.
type Checkpoint struct {
	bun.BaseModel `bun:"table:cqrs_checkpoints"`

	Group        string     `bun:"consumer_group,pk"`
	Topic        string     `bun:"topic,pk"`
	Partition    int32      `bun:"partition,pk"`
	NextOffset   int64      `bun:"next_offset,notnull"`
	LastRecordAt *time.Time `bun:"last_record_at"`
	UpdatedAt    time.Time  `bun:"updated_at,notnull"`
}

type (
	ICQRSService[TData any, TResponse any, TRequest any, TID comparable] interface {
		// Run checks that the write and read databases and the model's tables are
		// ready, then starts replicating the model's Debezium topic into the read
		// database. It returns once replication is running (it does not block);
		// Stop ends it. Run is a no-op while already running. The CDC service
		// itself is started separately (CDCService.Run), before or after.
		Run(ctx context.Context) error
		// Stop ends replication: the batch being applied finishes or rolls back
		// whole, its change hooks are waited for, and ctx bounds the wait.
		Stop(ctx context.Context) error
		// Stats is the running totals of the replication.
		Stats() Stats

		StartTx(ctx context.Context) (bun.Tx, error)
		EndTx(ctx context.Context, tx bun.Tx, err error) error

		OnCreated(ctx context.Context, data *TData)
		OnUpdated(ctx context.Context, data *TData)
		OnDeleted(ctx context.Context, data *TData)

		ToModel(data *TData) *TResponse
		ToModels(data []*TData) []*TResponse

		Create(ctx context.Context, data TData, preload ...string) (*TData, error)
		CreateMany(ctx context.Context, data []TData, preload ...string) ([]*TData, error)
		CreateWithTx(ctx context.Context, tx bun.Tx, data TData, preload ...string) (*TData, error)
		CreateManyWithTx(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TData, error)

		CreateWithValidation(ctx context.Context, request TRequest, preload ...string) (*TData, error)
		CreateManyWithValidation(ctx context.Context, requests []TRequest, preload ...string) ([]*TData, error)
		CreateWithValidationTx(ctx context.Context, tx bun.Tx, request TRequest, preload ...string) (*TData, error)
		CreateManyWithValidationTx(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TData, error)

		UpdateByID(ctx context.Context, id TID, data TData, preload ...string) (*TData, error)
		UpdateByIDWithTx(ctx context.Context, tx bun.Tx, id TID, data TData, preload ...string) (*TData, error)
		UpdateMany(ctx context.Context, data []TData, preload ...string) ([]*TData, error)
		UpdateManyFormat(ctx context.Context, data []TData, preload ...string) ([]*TResponse, error)
		UpdateManyWithTx(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TData, error)
		UpdateManyWithTxFormat(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TResponse, error)

		UpdateByIDWithValidation(ctx context.Context, id TID, request TRequest, preload ...string) (*TData, error)
		UpdateByIDWithValidationFormat(ctx context.Context, id TID, request TRequest, preload ...string) (*TResponse, error)
		UpdateByIDWithValidationTx(ctx context.Context, tx bun.Tx, id TID, request TRequest, preload ...string) (*TData, error)
		UpdateByIDWithValidationTxFormat(ctx context.Context, tx bun.Tx, id TID, request TRequest, preload ...string) (*TResponse, error)
		UpdateManyWithValidation(ctx context.Context, requests []TRequest, preload ...string) ([]*TData, error)
		UpdateManyWithValidationFormat(ctx context.Context, requests []TRequest, preload ...string) ([]*TResponse, error)
		UpdateManyWithValidationTx(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TData, error)
		UpdateManyWithValidationTxFormat(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TResponse, error)

		IncrementByID(ctx context.Context, id TID, field string, delta float64) (*TData, error)
		IncrementByIDWithTx(ctx context.Context, tx bun.Tx, id TID, field string, delta float64) (*TData, error)

		DeleteByID(ctx context.Context, id TID) error
		DeleteByIDWithTx(ctx context.Context, tx bun.Tx, id TID) error
		DeleteMany(ctx context.Context, ids []TID) error
		DeleteManyWithTx(ctx context.Context, tx bun.Tx, ids []TID) error

		GetByID(ctx context.Context, id TID, preloads ...string) (*TData, error)
		GetByIDFormat(ctx context.Context, id TID, preloads ...string) (*TResponse, error)
		GetByIDWithTx(ctx context.Context, tx *bun.Tx, id TID, preloads ...string) (*TData, error)
		GetByIDWithTxFormat(ctx context.Context, tx *bun.Tx, id TID, preloads ...string) (*TResponse, error)

		Find(ctx context.Context, filter pagination.StructuredFilter, preloads ...string) ([]*TData, error)
		FindFormat(ctx context.Context, filter pagination.StructuredFilter, preloads ...string) ([]*TResponse, error)
		FindWithTx(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, preloads ...string) ([]*TData, error)
		FindWithTxFormat(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, preloads ...string) ([]*TResponse, error)

		FindOne(ctx context.Context, filter pagination.StructuredFilter, preloads ...string) (*TData, error)
		FindOneFormat(ctx context.Context, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error)
		FindOneWithTx(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, preloads ...string) (*TData, error)
		FindOneWithTxFormat(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error)

		Count(ctx context.Context, filter pagination.StructuredFilter) (int64, error)
		CountWithTx(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) (int64, error)
		Exists(ctx context.Context, filter pagination.StructuredFilter) (bool, error)
		ExistsWithTx(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) (bool, error)

		Max(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error)
		MaxFormat(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error)
		MaxWithTx(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error)
		MaxWithTxFormat(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error)
		Min(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error)
		MinFormat(ctx context.Context, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error)
		MinWithTx(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TData, error)
		MinWithTxFormat(ctx context.Context, tx *bun.Tx, field string, filter pagination.StructuredFilter, preloads ...string) (*TResponse, error)

		Filter(ctx context.Context, filter pagination.StructuredFilter) ([]*TData, error)
		FilterFormat(ctx context.Context, filter pagination.StructuredFilter) ([]*TResponse, error)
		FilterWithTx(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) ([]*TData, error)
		FilterWithTxFormat(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter) ([]*TResponse, error)
		Paginate(ctx context.Context, page pagination.Pagination) (pagination.PaginationResult[TData], error)
		PaginateFormat(ctx context.Context, page pagination.Pagination) (pagination.PaginationResult[TResponse], error)
		PaginateFilter(ctx context.Context, filter pagination.StructuredFilter, page pagination.Pagination) (pagination.PaginationResult[TData], error)
		PaginateFilterFormat(ctx context.Context, filter pagination.StructuredFilter, page pagination.Pagination) (pagination.PaginationResult[TResponse], error)
		PaginateWithHertz(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, reqCtx *app.RequestContext) (pagination.PaginationResult[TData], error)
		PaginateWithHertzFormat(ctx context.Context, tx *bun.Tx, filter pagination.StructuredFilter, reqCtx *app.RequestContext) (pagination.PaginationResult[TResponse], error)
	}
)
