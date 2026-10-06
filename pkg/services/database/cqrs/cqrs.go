package cqrs

import (
	"reflect"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/go-playground/validator/v10"
)

type CQRSService[TData any, TResponse any, TRequest any, TID comparable] struct {
	Channel           broadcast.Channel
	ColumnDefaultID   string
	ColumnDefaultSort string
	Preloads          []string

	ToResource  func(*TData) *TResponse
	TocCSV      func(*TData) *map[string]any
	FromRequest func(*TRequest) *TData
	Created     func(*TData) broadcast.Events
	Updated     func(*TData) broadcast.Events
	Deleted     func(*TData) broadcast.Events
	Dispatch    func(channel broadcast.Channel, events broadcast.Events, payload *TResponse) error

	ReadSQLService       sql.SQLServices
	WriteSQLService      sql.SQLServices
	Log                  logger.LogContextService
	BroadcastService     broadcast.BroadcasterServices
	MessageBrokerService broker.MessageBrokerServices

	PaginationService pagination.PaginationServices[TData, TID]
	Validator         *validator.Validate

	BatchSize     int
	FlushInterval time.Duration

	stringSlicePool     *utils.BufferPool[string]
	stringSetPool       *utils.MapPool[string, bool]
	processedEventsPool *utils.BufferPool[ProcessedEvent]

	idFieldIndex int
	entity       string
}

func NewCQRS[TData any, TResponse any, TRequest any, TID comparable](
	c CQRSService[TData, TResponse, TRequest, TID],
) CQRSServices[TData, TResponse, TRequest, TID] {
	if c.WriteSQLService == nil {
		panic("WriteSQLService must be initialized")
	}
	if c.PaginationService == nil {
		panic("PaginationService must be initialized")
	}
	if c.ColumnDefaultID == "" {
		c.ColumnDefaultID = "id"
	}
	if c.ColumnDefaultSort == "" {
		c.ColumnDefaultSort = "updated_at DESC"
	}
	if c.Channel == "" {
		c.Channel = "default"
	}
	if c.Validator == nil {
		c.Validator = validator.New()
	}
	if c.BatchSize == 0 {
		c.BatchSize = 100
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = 5 * time.Second
	}
	return &CQRSService[TData, TResponse, TRequest, TID]{
		Channel:              c.Channel,
		ColumnDefaultID:      c.ColumnDefaultID,
		ColumnDefaultSort:    c.ColumnDefaultSort,
		Preloads:             c.Preloads,
		ToResource:           c.ToResource,
		TocCSV:               c.TocCSV,
		FromRequest:          c.FromRequest,
		Created:              c.Created,
		Updated:              c.Updated,
		Deleted:              c.Deleted,
		Dispatch:             c.Dispatch,
		ReadSQLService:       c.ReadSQLService,
		WriteSQLService:      c.WriteSQLService,
		Log:                  c.Log,
		BroadcastService:     c.BroadcastService,
		MessageBrokerService: c.MessageBrokerService,
		Validator:            c.Validator,
		stringSlicePool:      utils.NewBufferPool[string](),
		stringSetPool:        utils.NewMapPool[string, bool](),
		processedEventsPool:  utils.NewBufferPool[ProcessedEvent](),
		BatchSize:            c.BatchSize,
		FlushInterval:        c.FlushInterval,
		idFieldIndex:         utils.BunColumnFieldIndex[TData](c.ColumnDefaultID),
		entity:               reflect.TypeFor[TData]().String(),
		PaginationService:    c.PaginationService,
	}
}
