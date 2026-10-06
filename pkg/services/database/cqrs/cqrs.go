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
	// ColumnVersion, when set (e.g. "updated_at" or "version"), makes the read-model sync keep the
	// newest version of a row: an older change that arrives late never overwrites a newer one.
	ColumnVersion string
	Preloads      []string

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

	// MaxRetries bounds how often a batch that failed for a transient reason (database unreachable,
	// timeout, serialization conflict) is retried before the runner gives up and stops without
	// acknowledging it, so it is redelivered on restart. 0 retries until the runner is stopped.
	MaxRetries int
	// RetryBackoff is the first retry delay; it doubles up to 5s. Default 100ms.
	RetryBackoff time.Duration
	// DLQTopic receives messages that can never be applied (malformed, or rejected by the read
	// model), so they are kept instead of lost. Default "<Channel>.dlq"; "-" disables it.
	DLQTopic string
	// RawText keeps text exactly as given: no NFC normalization and no rejection of U+FFFD, control or
	// BiDi characters (invalid UTF-8 and NUL bytes are still refused). See cqrs.text.go.
	RawText bool
	// HookWorkers bounds how many change hooks (Dispatch / Broadcast) run at once. Default 64.
	HookWorkers int

	stringSlicePool     *utils.BufferPool[string]
	stringSetPool       *utils.MapPool[string, bool]
	processedEventsPool *utils.BufferPool[ProcessedEvent]

	idFieldIndex      int
	versionFieldIndex int
	textFields        []int
	columnFields      map[string]int
	hooks             *hookPool
	entity            string
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
	if c.RetryBackoff == 0 {
		c.RetryBackoff = 100 * time.Millisecond
	}
	if c.DLQTopic == "" {
		c.DLQTopic = string(c.Channel) + ".dlq"
	}
	return &CQRSService[TData, TResponse, TRequest, TID]{
		Channel:              c.Channel,
		ColumnDefaultID:      c.ColumnDefaultID,
		ColumnDefaultSort:    c.ColumnDefaultSort,
		ColumnVersion:        c.ColumnVersion,
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
		versionFieldIndex:    versionFieldIndex[TData](c.ColumnVersion),
		textFields:           textFieldIndexes[TData](),
		columnFields:         columnFieldIndexes[TData](),
		hooks:                newHookPool(c.HookWorkers),
		HookWorkers:          c.HookWorkers,
		RawText:              c.RawText,
		MaxRetries:           c.MaxRetries,
		RetryBackoff:         c.RetryBackoff,
		DLQTopic:             c.DLQTopic,
		entity:               reflect.TypeFor[TData]().String(),
		PaginationService:    c.PaginationService,
	}
}
