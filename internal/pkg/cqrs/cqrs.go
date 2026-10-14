package cqrs

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
	"github.com/go-playground/validator/v10"
)

// CQRSService is the typed service of one model: writes go to WriteSQLService,
// reads go to the read database, and Run keeps the read database in step with
// the write database by applying its Debezium change stream (see Run).
//
// Build it with NewCQRS; the zero value is not usable (its methods return
// ErrNotConstructed).
type CQRSService[TData any, TResponse any, TRequest any, TID comparable] struct {
	// Channel names the broadcast channel the change hooks publish on.
	Channel string
	// Topic is the Debezium topic of the model's table. When empty it is
	// derived from the model: CDCService.Topic(schema, table).
	Topic             string
	ColumnDefaultID   string
	ColumnDefaultSort string
	// ColumnVersion, when set, names a column (integer or time) that only
	// grows as a row changes. The read database then never lets an older
	// version overwrite a newer one, whatever order the changes arrive in.
	ColumnVersion string
	// OrderByLSN orders every row's changes by their position in the write
	// database's WAL (the event's source.lsn) instead of by ColumnVersion. The
	// read database remembers the position of the last change of each row, delete
	// included (table cqrs_row_state, created with RowStateDDL), so an older change
	// is turned away whatever happens to the stream: a replay of an old window, a
	// restored snapshot, a row deleted and recreated (which restarts a version
	// counter at 1 but never moves the WAL back). It also orders models that have
	// no version column. Events that carry no LSN fall back to ColumnVersion.
	OrderByLSN bool
	Preloads   []string

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

	// Logger receives the service's structured logs (slog.Default if nil).
	Logger *slog.Logger
	// BatchSize caps the changes applied in one read-database transaction.
	BatchSize int
	// ApplyTimeout bounds one such transaction.
	ApplyTimeout time.Duration
	// RetryBackoff is the first wait after a failed attempt, doubling up to
	// MaxRetryBackoff. A change that cannot be applied is retried until it can
	// (or is dead-lettered when the database says it never can), never skipped.
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
	RawText         bool
	// HookWorkers bounds how many change hooks run at once.
	HookWorkers int
	// RepairInterval is how often the service looks again at rows whose changes
	// the read database rejected for good (they are parked on the dead-letter
	// topic) and writes their current state once the read database accepts it:
	// the repair is automatic, nothing has to be called. Default 30s; negative
	// turns it off.
	RepairInterval time.Duration
	// RowStateRetention is how long OrderByLSN keeps the tombstone of a deleted
	// row before the service prunes it. It must be longer than anything that can
	// still be replayed, so keep it above the topic's Kafka retention. Default 14
	// days; negative never prunes.
	RowStateRetention time.Duration

	// Everything below is set by NewCQRS. The mutable parts live behind
	// pointers so the struct can be passed by value (as NewCQRS does) without
	// copying a lock.
	model      *model[TData]
	topic      string
	log        *slog.Logger
	st         *state
	textFields []int

	// Scratch space of the apply path, reused across batches. Both pools
	// clear what they take back, so a batch never sees the last one's rows.
	keyPool   *utils.MapPool[string, int]
	groupPool *utils.BufferPool[group[TData]]
}

// state is what changes while the service runs.
type state struct {
	lifecycle sync.Mutex // serialises Run and Stop
	gate      *utils.Gate
	hooks     *utils.Inflight // change hooks still running
	hookSlots chan struct{}   // one token per running hook
	repair    *repairState    // rows parked for good, waiting to be repaired
	stats     counters
}

// counters are the service's running totals; Stats reads them.
type counters struct {
	batches      atomic.Int64
	records      atomic.Int64
	applied      atomic.Int64
	duplicates   atomic.Int64
	tombstones   atomic.Int64
	skipped      atomic.Int64
	deadLettered atomic.Int64
	retries      atomic.Int64
	stale        atomic.Int64
	commitFails  atomic.Int64
	hooks        atomic.Int64
	hooksDropped atomic.Int64
	repaired     atomic.Int64
	lastError    atomic.Pointer[string]
}

// Stats is a snapshot of the totals since the service was created.
type Stats struct {
	Batches      int64  // batches received from the change stream
	Records      int64  // records received
	Applied      int64  // row changes written to the read database
	Duplicates   int64  // records skipped because the checkpoint already covered them
	Tombstones   int64  // Debezium tombstones skipped
	Skipped      int64  // records skipped by design (truncates)
	DeadLettered int64  // records that can never be applied, parked on the dead-letter topic
	Retries      int64  // failed attempts that will be retried
	Stale        int64  // upserts turned away because the stored row had a newer version
	CommitFails  int64  // Kafka offset commits that failed (the records are delivered again, then skipped)
	Hooks        int64  // change hooks run
	HooksDropped int64  // change hooks not run because the service was stopping
	Repaired     int64  // rows the automatic repair brought in line with the write database
	Awaiting     int64  // parked rows still waiting for the read database to accept them
	LastError    string // the latest apply error, or ""
}

func (c *counters) fail(err error) {
	c.retries.Add(1)
	msg := err.Error()
	c.lastError.Store(&msg)
}

// Stats returns the service's running totals.
func (c *CQRSService[TData, TResponse, TRequest, TID]) Stats() Stats {
	if c == nil || c.st == nil {
		return Stats{}
	}
	s := &c.st.stats
	out := Stats{
		Batches:      s.batches.Load(),
		Records:      s.records.Load(),
		Applied:      s.applied.Load(),
		Duplicates:   s.duplicates.Load(),
		Tombstones:   s.tombstones.Load(),
		Skipped:      s.skipped.Load(),
		DeadLettered: s.deadLettered.Load(),
		Retries:      s.retries.Load(),
		Stale:        s.stale.Load(),
		CommitFails:  s.commitFails.Load(),
		Hooks:        s.hooks.Load(),
		HooksDropped: s.hooksDropped.Load(),
		Repaired:     s.repaired.Load(),
	}
	if c.st.repair != nil {
		out.Awaiting = int64(c.st.repair.count())
	}
	if msg := s.lastError.Load(); msg != nil {
		out.LastError = *msg
	}
	return out
}

func NewCQRS[TData any, TResponse any, TRequest any, TID comparable](
	c CQRSService[TData, TResponse, TRequest, TID], inflight *utils.Inflight,
) ICQRSService[TData, TResponse, TRequest, TID] {
	if utils.IsNil(c.WriteSQLService) {
		panic("WriteSQLService must be initialized")
	}
	if utils.IsNil(c.PaginationService) {
		panic("PaginationService must be initialized")
	}
	if utils.IsNil(c.CDCService) {
		panic("CDCService must be initialized")
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
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.ApplyTimeout <= 0 {
		c.ApplyTimeout = defaultApplyTimeout
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = defaultRetryBackoff
	}
	if c.MaxRetryBackoff < c.RetryBackoff {
		c.MaxRetryBackoff = max(c.RetryBackoff, defaultMaxBackoff)
	}
	if c.HookWorkers <= 0 {
		c.HookWorkers = defaultHookWorkers
	}
	if c.RepairInterval == 0 {
		c.RepairInterval = defaultRepairInterval
	}
	if c.RowStateRetention == 0 {
		c.RowStateRetention = defaultRowStateRetention
	}

	// A model the replicator cannot key is a programming error: fail at
	// construction, not on the first change.
	m, err := newModel[TData](c.ColumnDefaultID, c.ColumnVersion)
	if err != nil {
		panic(fmt.Sprintf("cqrs: %v", err))
	}
	topic := c.Topic
	if topic == "" {
		topic = c.CDCService.Topic(m.schema, m.name)
	}
	base := c.Logger
	if base == nil {
		base = slog.Default()
	}

	return &CQRSService[TData, TResponse, TRequest, TID]{
		Channel:           c.Channel,
		Topic:             c.Topic,
		ColumnDefaultID:   c.ColumnDefaultID,
		ColumnDefaultSort: c.ColumnDefaultSort,
		ColumnVersion:     c.ColumnVersion,
		OrderByLSN:        c.OrderByLSN,
		Preloads:          c.Preloads,
		ToResource:        c.ToResource,
		TocCSV:            c.TocCSV,
		FromRequest:       c.FromRequest,
		Created:           c.Created,
		Updated:           c.Updated,
		Deleted:           c.Deleted,
		Dispatch:          c.Dispatch,
		Validator:         c.Validator,
		ReadSQLService:    c.ReadSQLService,
		WriteSQLService:   c.WriteSQLService,
		CDCService:        c.CDCService,
		BroadcastService:  c.BroadcastService,
		PaginationService: c.PaginationService,
		Logger:            c.Logger,
		BatchSize:         c.BatchSize,
		ApplyTimeout:      c.ApplyTimeout,
		RetryBackoff:      c.RetryBackoff,
		MaxRetryBackoff:   c.MaxRetryBackoff,
		RawText:           c.RawText,
		HookWorkers:       c.HookWorkers,
		RepairInterval:    c.RepairInterval,
		RowStateRetention: c.RowStateRetention,

		model:      m,
		topic:      topic,
		log:        base.With("component", "cqrs", "channel", c.Channel, "topic", topic),
		textFields: utils.TextFieldIndexes[TData](),
		keyPool:    utils.NewMapPool[string, int](),
		groupPool:  utils.NewBufferPool[group[TData]](),
		st: &state{
			gate:      utils.NewGate(inflight),
			hooks:     utils.NewInflight(),
			hookSlots: make(chan struct{}, c.HookWorkers),
			repair:    newRepairState(),
		},
	}
}
