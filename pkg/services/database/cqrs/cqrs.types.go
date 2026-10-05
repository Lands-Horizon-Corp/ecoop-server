package cqrs

import (
	"context"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/uptrace/bun"
)

type ChangeType int

const (
	ChangeTypeCreated ChangeType = iota + 1
	ChangeTypeUpdated
	ChangeTypeDeleted
)

type (
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

	CQRSServices[TData any, TResponse any, TRequest any, TID comparable] interface {
		Run(ctx context.Context) error

		StartTx(ctx context.Context) (bun.Tx, error)
		EndTx(ctx context.Context, tx bun.Tx, err error) error

		OnCreated(ctx context.Context, data *TData)
		OnUpdated(ctx context.Context, data *TData)
		OnDeleted(ctx context.Context, data *TData)

		ToModel(data *TData) *TResponse
		ToModels(data []*TData) []*TResponse

		Create(ctx context.Context, data TData, preload ...string) (*TData, error)
		CreateFormat(ctx context.Context, data TData, preload ...string) (*TResponse, error)
		CreateMany(ctx context.Context, data []TData, preload ...string) ([]*TData, error)
		CreateManyFormat(ctx context.Context, data []TData, preload ...string) ([]*TResponse, error)
		CreateWithTx(ctx context.Context, tx bun.Tx, data TData, preload ...string) (*TData, error)
		CreateWithTxFormat(ctx context.Context, tx bun.Tx, data TData, preload ...string) (*TResponse, error)
		CreateManyWithTx(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TData, error)
		CreateManyWithTxFormat(ctx context.Context, tx bun.Tx, data []TData, preload ...string) ([]*TResponse, error)

		CreateWithValidation(ctx context.Context, request TRequest, preload ...string) (*TData, error)
		CreateWithValidationFormat(ctx context.Context, request TRequest, preload ...string) (*TResponse, error)
		CreateManyWithValidation(ctx context.Context, requests []TRequest, preload ...string) ([]*TData, error)
		CreateManyWithValidationFormat(ctx context.Context, requests []TRequest, preload ...string) ([]*TResponse, error)
		CreateWithValidationTx(ctx context.Context, tx bun.Tx, request TRequest, preload ...string) (*TData, error)
		CreateWithValidationTxFormat(ctx context.Context, tx bun.Tx, request TRequest, preload ...string) (*TResponse, error)
		CreateManyWithValidationTx(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TData, error)
		CreateManyWithValidationTxFormat(ctx context.Context, tx bun.Tx, requests []TRequest, preload ...string) ([]*TResponse, error)

		UpdateByID(ctx context.Context, id TID, data TData, preload ...string) (*TData, error)
		UpdateByIDFormat(ctx context.Context, id TID, data TData, preload ...string) (*TResponse, error)
		UpdateByIDWithTx(ctx context.Context, tx bun.Tx, id TID, data TData, preload ...string) (*TData, error)
		UpdateByIDWithTxFormat(ctx context.Context, tx bun.Tx, id TID, data TData, preload ...string) (*TResponse, error)
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
