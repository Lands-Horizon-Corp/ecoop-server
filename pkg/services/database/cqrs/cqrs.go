package cqrs

import (
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/go-playground/validator/v10"
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

	PaginationService database.PaginationServices[TData, TID]
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
		LogService:           c.LogService,
		BroadcastService:     c.BroadcastService,
		MessageBrokerService: c.MessageBrokerService,
		Validator:            c.Validator,
		stringSlicePool:      utils.NewBufferPool[string](),
		stringSetPool:        utils.NewMapPool[string, bool](),
		processedEventsPool:  utils.NewBufferPool[database.ProcessedEvent](),
		BatchSize:            c.BatchSize,
		FlushInterval:        c.FlushInterval,
		idFieldIndex:         utils.BunColumnFieldIndex[TData](c.ColumnDefaultID),
		PaginationService:    c.PaginationService,
	}
}
