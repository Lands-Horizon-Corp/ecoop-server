package cqrs

import (
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/go-playground/validator/v10"
)

type CQRSService[TData any, TResponse any, TRequest any, TID comparable] struct {
	// Channel and column configuration for the CQRS service.
	Channel           string
	ColumnDefaultID   string
	ColumnDefaultSort string
	ColumnVersion     string
	Preloads          []string

	// Functions for converting between data, requests, and responses, as well as handling events.
	ToResource  func(*TData) *TResponse
	TocCSV      func(*TData) *map[string]any
	FromRequest func(*TRequest) *TData
	Created     func(*TData) []string
	Updated     func(*TData) []string
	Deleted     func(*TData) []string
	Dispatch    func(channel string, events []string, payload *TResponse) error
	Validator   *validator.Validate

	// Services for handling SQL operations, CDC, and pagination.
	ReadSQLService    sql.ISQLService
	WriteSQLService   sql.ISQLService
	CDCService        cdc.ICDCService
	BroadcastService  broadcast.IBroadcastService
	PaginationService pagination.IPaginationService[TData, TID]

	BatchSize     int
	FlushInterval time.Duration
	MaxRetries    int
	RetryBackoff  time.Duration
	DLQTopic      string
	RawText       bool
	HookWorkers   int

	stringSlicePool *utils.BufferPool[string]
	stringSetPool   *utils.MapPool[string, bool]

	textFields []int
}

func NewCQRS[TData any, TResponse any, TRequest any, TID comparable](
	c CQRSService[TData, TResponse, TRequest, TID],
) ICQRSService[TData, TResponse, TRequest, TID] {
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
	if c.PaginationService == nil {
		panic("PaginationService must be initialized")
	}
	if c.CDCService == nil {
		panic("CDCService must be initialized")
	}

	return &CQRSService[TData, TResponse, TRequest, TID]{
		Channel:           c.Channel,
		ColumnDefaultID:   c.ColumnDefaultID,
		ColumnDefaultSort: c.ColumnDefaultSort,
		ColumnVersion:     c.ColumnVersion,
		Preloads:          c.Preloads,
		ToResource:        c.ToResource,
		TocCSV:            c.TocCSV,
		FromRequest:       c.FromRequest,
		Created:           c.Created,
		Updated:           c.Updated,
		Deleted:           c.Deleted,
		Dispatch:          c.Dispatch,
		ReadSQLService:    c.ReadSQLService,
		WriteSQLService:   c.WriteSQLService,
		CDCService:        c.CDCService,
		Validator:         c.Validator,

		BatchSize:     c.BatchSize,
		FlushInterval: c.FlushInterval,

		HookWorkers:       c.HookWorkers,
		RawText:           c.RawText,
		MaxRetries:        c.MaxRetries,
		RetryBackoff:      c.RetryBackoff,
		DLQTopic:          c.DLQTopic,
		PaginationService: c.PaginationService,

		textFields:      utils.TextFieldIndexes[TData](),
		stringSlicePool: utils.NewBufferPool[string](),
		stringSetPool:   utils.NewMapPool[string, bool](),
	}
}
