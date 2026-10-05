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
