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
