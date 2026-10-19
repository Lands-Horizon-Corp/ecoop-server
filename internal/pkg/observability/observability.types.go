package observability

import (
	"context"
	"errors"
	"log/slog"
)

var ErrNilContext = errors.New("observability: nil context")

// This file is the contract every package observes through. A package takes an
// IObserver (nil-checked at construction, like its other dependencies) and calls
// it; it never imports a telemetry library. What sits behind the interface (an
// OpenTelemetry adapter, a test double, nothing at all) is chosen where the
// services are wired, so instrumenting a package never changes its imports.

// Component names the package an instrument belongs to. It is a bounded set:
// only these values may appear as the "component" attribute.
type Component string

const (
	ComponentAPI        Component = "api"
	ComponentAuth       Component = "auth"
	ComponentBroadcast  Component = "broadcast"
	ComponentBroker     Component = "broker"
	ComponentCache      Component = "cache" // the Redis/Valkey client
	ComponentCDC        Component = "cdc"
	ComponentConfig     Component = "config"
	ComponentCQRS       Component = "cqrs"
	ComponentCron       Component = "cron"
	ComponentOTP        Component = "otp"
	ComponentPagination Component = "pagination"
	ComponentPDF        Component = "pdf"
	ComponentQR         Component = "qr"
	ComponentQueue      Component = "queue"
	ComponentSecurity   Component = "security"
	ComponentSMS        Component = "sms"
	ComponentSMTP       Component = "smtp"
	ComponentSQL        Component = "sql"
	ComponentStorage    Component = "storage"
)

// Outcome is how an operation ended. It is a bounded set, so it is safe as a
// metric label.
type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomeError   Outcome = "error"
	OutcomeRetry   Outcome = "retry"   // failed, and will be tried again
	OutcomeDLQ     Outcome = "dlq"     // parked on a dead-letter topic
	OutcomeSkipped Outcome = "skipped" // nothing to do: a duplicate, a tombstone
)

// Attribute keys. Metric labels must come from this list and stay bounded:
// never put a row key, member, account, transaction or request ID, a raw URL
// or a SQL statement in a label. Such values may go in a log or a span, where
// access and retention are controlled.
const (
	KeyComponent     = "component"
	KeyOperation     = "operation"
	KeyOutcome       = "outcome"
	KeyErrorType     = "error_type" // a bounded class, such as a sentinel's name
	KeyTopic         = "topic"
	KeyConsumerGroup = "consumer_group"
	KeyTable         = "table"
	KeyCacheFamily   = "cache_family" // what the cache holds, never the key itself
	KeyOverflow      = "overflow"     // "true" on the series that absorbs what the series cap refused
)

// Limits that keep the registry bounded whatever a caller passes.
const (
	MaxLabelValueLen = 128  // bytes; longer values are cut on a character boundary
	MaxSeries        = 2000 // series in the registry; past it, records fold into an overflow series
)

// Names of the instruments every package shares. One counter and one histogram
// cover every operation of every package, told apart by the component,
// operation and outcome attributes, so a new package is observable without
// defining new metrics.
const (
	MetricOperations        = "operations_total"           // counter
	MetricOperationDuration = "operation_duration_seconds" // histogram, seconds
	MetricInflight          = "inflight_operations"        // gauge: what utils.Inflight counts
)

// Names of the instruments that mirror cqrs.Stats, the one set of totals the
// code keeps today. The counters are totals since the service started.
const (
	MetricCQRSBatches       = "cqrs_batches_total"
	MetricCQRSRecords       = "cqrs_records_total"
	MetricCQRSApplied       = "cqrs_applied_total"
	MetricCQRSDuplicates    = "cqrs_duplicates_total"
	MetricCQRSTombstones    = "cqrs_tombstones_total"
	MetricCQRSSkipped       = "cqrs_skipped_total"
	MetricCQRSDeadLettered  = "cqrs_dead_lettered_total"
	MetricCQRSRetries       = "cqrs_retries_total"
	MetricCQRSStale         = "cqrs_stale_total"
	MetricCQRSCommitFails   = "cqrs_commit_failures_total"
	MetricCQRSHooks         = "cqrs_hooks_total"
	MetricCQRSHooksDropped  = "cqrs_hooks_dropped_total"
	MetricCQRSRepaired      = "cqrs_repaired_total"
	MetricCQRSAwaiting      = "cqrs_awaiting_rows"          // gauge: parked rows still waiting for repair
	MetricCQRSBatchDuration = "cqrs_batch_duration_seconds" // histogram, seconds
)

type (
	// Attr is one key and value. It is slog's own type, so the same constructors
	// (slog.String, slog.Int, ...) work for logs, spans and metrics.
	Attr = slog.Attr

	// Resource says which process is reporting. Every field is bounded, so all of
	// it may appear on every signal.
	Resource struct {
		ServiceName string // stable per deployable, for example "ecoop-server"
		Namespace   string // the product, for example "e-coop"
		Version     string // the release or build
		Environment string // development, staging or production
		InstanceID  string // optional: one running copy
		Revision    string // optional: the Git commit
	}

	// Counter only goes up.
	Counter interface {
		Add(ctx context.Context, delta int64, attrs ...Attr)
	}

	// Gauge is a value that goes up and down.
	Gauge interface {
		Set(ctx context.Context, value float64, attrs ...Attr)
	}

	// Histogram records a distribution, such as a duration in seconds.
	Histogram interface {
		Record(ctx context.Context, value float64, attrs ...Attr)
	}

	// Meter creates the instruments of one component. Asking twice for the same
	// name returns an instrument that adds to the same series.
	Meter interface {
		Counter(name, description string) Counter
		Gauge(name, description string) Gauge
		Histogram(name, description, unit string) Histogram
	}

	// Span is one timed step of a trace.
	Span interface {
		SetAttributes(attrs ...Attr)
		// RecordError attaches err to the span. Pass the error as returned, and
		// keep row values out of it, as everywhere else.
		RecordError(err error)
		End()
	}

	// Tracer starts spans. The context it returns carries the span, so a span
	// started from it is its child, across packages.
	Tracer interface {
		Start(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
	}

	// Operation is one unit of work seen as a whole: a span, a count and a
	// duration, ended with one call. It is how a package instruments an operation
	// in two lines, and the same everywhere:
	//
	//	ctx, op := obs.Begin(ctx, observability.ComponentCache, "get")
	//	defer func() { op.End(err) }()
	Operation interface {
		SetAttributes(attrs ...Attr)
		// SetOutcome overrides the outcome End would pick (success for a nil
		// error, error otherwise): a retried or dead-lettered operation ends in
		// OutcomeRetry or OutcomeDLQ, a duplicate in OutcomeSkipped.
		SetOutcome(outcome Outcome)
		// End closes the span, adds one to MetricOperations and records the
		// elapsed time in MetricOperationDuration, with the component, operation
		// and outcome attributes. It is safe to call once only.
		End(err error)
	}

	// IObserver is what a package is given. Every method is safe for concurrent
	// use. A package must not fail or block because telemetry is unavailable.
	IObserver interface {
		// Logger is a structured logger carrying the component attribute. Log at
		// the top of a unit of work, once per error, with stable messages and the
		// variable data as attributes.
		Logger(component Component) *slog.Logger
		// Meter creates the component's own instruments, for totals the generic
		// operation metrics do not cover (for example cqrs.Stats).
		Meter(component Component) Meter
		// Tracer starts the component's own spans.
		Tracer(component Component) Tracer
		// Begin starts an Operation of the component.
		Begin(ctx context.Context, component Component, operation string, attrs ...Attr) (context.Context, Operation)
	}

	// IObservabilityService is the observer plus its lifecycle, like the other
	// services: Start connects the exporters, Stop flushes what is buffered. The
	// service is created where the services are wired, and only the IObserver is
	// passed on.
	IObservabilityService interface {
		IObserver
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
	}
)
