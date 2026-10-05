package cqrs

import (
	"context"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/go-playground/validator/v10"
	"github.com/uptrace/bun"
)

type CQRSService[TData any, TResponse any, TRequest any, TID comparable] struct {
	Channel           database.Channel
	ColumnDefaultID   string
	ColumnDefaultSort string
	Preloads          []string

	ToResource  func(*TData) *TResponse
	TocCSV      func(*TData) *map[string]any
	FromRequest func(*TRequest) *TData
	Created     func(*TData) database.Events
	Updated     func(*TData) database.Events
	Deleted     func(*TData) database.Events
	Dispatch    func(channel database.Channel, events database.Events, payload *TResponse) error

	ReadSQLService       database.SQLService
	WriteSQLService      database.SQLService
	LogService           database.LogService
	BroadcastService     database.BroadcastService
	MessageBrokerService database.MessageBrokerService

	PaginationService database.PaginationService[TData, TID]
	Validator         *validator.Validate

	BatchSize     int
	FlushInterval time.Duration

	stringSlicePool     *utils.BufferPool[string]
	stringSetPool       *utils.MapPool[string, bool]
	processedEventsPool *utils.BufferPool[database.ProcessedEvent]

	idFieldIndex int
}

func NewCQRS[TData any, TResponse any, TRequest any, TID comparable](
	c CQRSService[TData, TResponse, TRequest, TID],
) database.CQRSServices[TData, TResponse, TRequest, TID] {
	return &CQRSService[TData, TResponse, TRequest, TID]{}
}

// Transactions
func (c *CQRSService[TData, TResponse, TRequest, TID]) Start(ctx context.Context) (bun.Tx, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) End(ctx context.Context, tx bun.Tx, err error) error {
	panic("not implemented")
}

// Model conversion
func (c *CQRSService[TData, TResponse, TRequest, TID]) ToModel(data *TData) *TResponse {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) ToModels(data []*TData) []*TResponse {
	panic("not implemented")
}

// Create
func (c *CQRSService[TData, TResponse, TRequest, TID]) Create(ctx context.Context, data TData, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateFormat(ctx context.Context, data TData, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateMany(ctx context.Context, data []TData, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyFormat(ctx context.Context, data []TData, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithTx(ctx context.Context, tx bun.Tx, data TData, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithTxFormat(ctx context.Context, tx bun.Tx, data TData, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithTx(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithTxFormat(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

// Create with validation
func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidation(ctx context.Context, request TRequest, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationFormat(ctx context.Context, request TRequest, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidation(ctx context.Context, requests []TRequest, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationFormat(ctx context.Context, requests []TRequest, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationTx(ctx context.Context, tx bun.Tx, request TRequest, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateWithValidationTxFormat(ctx context.Context, tx bun.Tx, request TRequest, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationTx(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CreateManyWithValidationTxFormat(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

// Update
func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByID(ctx context.Context, id TID, data TData, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDFormat(ctx context.Context, id TID, data TData, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithTx(ctx context.Context, tx bun.Tx, id TID, data TData, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithTxFormat(ctx context.Context, tx bun.Tx, id TID, data TData, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateMany(ctx context.Context, data []TData, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyFormat(ctx context.Context, data []TData, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithTx(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithTxFormat(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

// Update with validation
func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidation(ctx context.Context, id TID, request TRequest, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationFormat(ctx context.Context, id TID, request TRequest, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationTx(ctx context.Context, tx bun.Tx, id TID, request TRequest, preload ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateByIDWithValidationTxFormat(ctx context.Context, tx bun.Tx, id TID, request TRequest, preload ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidation(ctx context.Context, requests []TRequest, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationFormat(ctx context.Context, requests []TRequest, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationTx(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) UpdateManyWithValidationTxFormat(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TResponse, error) {
	panic("not implemented")
}

// Increment
func (c *CQRSService[TData, TResponse, TRequest, TID]) IncrementByID(ctx context.Context, id TID, field string, delta float64) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) IncrementByIDWithTx(ctx context.Context, tx bun.Tx, id TID, field string, delta float64) (*TData, error) {
	panic("not implemented")
}

// Delete
func (c *CQRSService[TData, TResponse, TRequest, TID]) DeleteByID(ctx context.Context, id TID) error {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) DeleteByIDWithTx(ctx context.Context, tx bun.Tx, id TID) error {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) DeleteMany(ctx context.Context, ids []TID) error {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) DeleteManyWithTx(ctx context.Context, tx bun.Tx, ids []TID) error {
	panic("not implemented")
}

// Get by ID
func (c *CQRSService[TData, TResponse, TRequest, TID]) GetByID(ctx context.Context, id TID, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) GetByIDFormat(ctx context.Context, id TID, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) GetByIDWithTx(ctx context.Context, tx *bun.Tx, id TID, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) GetByIDWithTxFormat(ctx context.Context, tx *bun.Tx, id TID, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

// Find
func (c *CQRSService[TData, TResponse, TRequest, TID]) Find(ctx context.Context, filter database.StructuredFilter, preloads ...string) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindFormat(ctx context.Context, filter database.StructuredFilter, preloads ...string) ([]*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindWithTx(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindWithTxFormat(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string) ([]*TResponse, error) {
	panic("not implemented")
}

// FindOne
func (c *CQRSService[TData, TResponse, TRequest, TID]) FindOne(ctx context.Context, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindOneFormat(ctx context.Context, filter database.StructuredFilter, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindOneWithTx(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FindOneWithTxFormat(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

// Count / Exists
func (c *CQRSService[TData, TResponse, TRequest, TID]) Count(ctx context.Context, filter database.StructuredFilter) (int64, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) CountWithTx(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter) (int64, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) Exists(ctx context.Context, filter database.StructuredFilter) (bool, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) ExistsWithTx(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter) (bool, error) {
	panic("not implemented")
}

// Max / Min
func (c *CQRSService[TData, TResponse, TRequest, TID]) Max(ctx context.Context, field string, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MaxFormat(ctx context.Context, field string, filter database.StructuredFilter, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MaxWithTx(ctx context.Context, tx *bun.Tx, field string, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MaxWithTxFormat(ctx context.Context, tx *bun.Tx, field string, filter database.StructuredFilter, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) Min(ctx context.Context, field string, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MinFormat(ctx context.Context, field string, filter database.StructuredFilter, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MinWithTx(ctx context.Context, tx *bun.Tx, field string, filter database.StructuredFilter, preloads ...string) (*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) MinWithTxFormat(ctx context.Context, tx *bun.Tx, field string, filter database.StructuredFilter, preloads ...string) (*TResponse, error) {
	panic("not implemented")
}

// Filter /  database.Pagination
func (c *CQRSService[TData, TResponse, TRequest, TID]) Filter(ctx context.Context, filter database.StructuredFilter) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FilterFormat(ctx context.Context, filter database.StructuredFilter) ([]*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FilterWithTx(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter) ([]*TData, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) FilterWithTxFormat(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter) ([]*TResponse, error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) Paginate(ctx context.Context, pagination database.Pagination) (database.PaginationResult[TData], error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateFormat(ctx context.Context, pagination database.Pagination) (database.PaginationResult[TResponse], error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateFilter(ctx context.Context, filter database.StructuredFilter, pagination database.Pagination) (database.PaginationResult[TData], error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateFilterFormat(ctx context.Context, filter database.StructuredFilter, pagination database.Pagination) (database.PaginationResult[TResponse], error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateWithHertz(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, reqCtx *app.RequestContext) (database.PaginationResult[TData], error) {
	panic("not implemented")
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) PaginateWithHertzFormat(ctx context.Context, tx *bun.Tx, filter database.StructuredFilter, reqCtx *app.RequestContext) (database.PaginationResult[TResponse], error) {
	panic("not implemented")
}
