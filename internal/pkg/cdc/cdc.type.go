package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Debezium operation codes (the envelope's "op").
const (
	OpCreate   ChangeType = "c"
	OpUpdate   ChangeType = "u"
	OpDelete   ChangeType = "d"
	OpRead     ChangeType = "r" // a row read during the initial snapshot
	OpTruncate ChangeType = "t"
)

// Delivery settings.
const (
	defaultTopicPrefix = "ecoop"
	defaultDLQSuffix   = ".dlq"
	defaultAttempts    = 3
	defaultBackoff     = 200 * time.Millisecond
	maxDLQBackoff      = 5 * time.Second // cap while the DLQ itself is unreachable

	// DefaultMaxBatch caps how many records of one partition SubscribeBatch
	// hands to a handler at once.
	DefaultMaxBatch = 500

	// DefaultMaxDeadLetters caps how many records DeadLetters returns when the
	// caller does not say.
	DefaultMaxDeadLetters = 100_000

	// deadLettersTimeout bounds DeadLetters when its context has no deadline.
	deadLettersTimeout = 2 * time.Minute
)

// Headers added to every dead-lettered record. The original record's own
// headers (Debezium sets some) are kept.
const (
	HeaderDLQTopic     = "dlq.original-topic"
	HeaderDLQPartition = "dlq.original-partition"
	HeaderDLQOffset    = "dlq.original-offset"
	HeaderDLQError     = "dlq.error"
	HeaderDLQAttempts  = "dlq.attempts"
	HeaderDLQGroup     = "dlq.group"
	HeaderDLQFailedAt  = "dlq.failed-at"
)

var (
	ErrNoService             = errors.New("cdc: service is not set up; create it with NewCDCService")
	ErrNoBrokers             = errors.New("cdc: no Kafka address (set BROKER_BOOTSTRAP_SERVERS or BROKER_HOST and BROKER_PORT)")
	ErrNoGroup               = errors.New("cdc: no consumer group (set BROKER_GROUP_ID)")
	ErrNoTopic               = errors.New("cdc: topic is empty")
	ErrNoHandler             = errors.New("cdc: handler is nil")
	ErrDuplicateSubscription = errors.New("cdc: topic already subscribed")
	ErrConnect               = errors.New("cdc: cannot reach Kafka")
	ErrNotChangeEvent        = errors.New("cdc: value is not a Debezium change event")
	ErrNoBatchHandler        = errors.New("cdc: batch handler is nil")
	ErrNilContext            = errors.New("cdc: nil context")
	ErrNotRunning            = errors.New("cdc: service is not running")
	ErrTooManyDeadLetters    = errors.New("cdc: the dead-letter topic holds more records than the limit")
)

type (
	ChangeType  string
	ChangeEvent struct {
		Op     ChangeType      `json:"op"`
		Before json.RawMessage `json:"before"`
		After  json.RawMessage `json:"after"`
		Source struct {
			DB     string `json:"db"`
			Schema string `json:"schema"`
			Table  string `json:"table"`
			LSN    int64  `json:"lsn"`
			TxID   int64  `json:"txId"`
		} `json:"source"`
		TsMs int64 `json:"ts_ms"`
	}

	// Handler processes one change. key is the row's primary key as JSON,
	// e.g. {"id":7}. Return nil when done; an error retries it, and when the
	// attempts run out the change is parked on the topic's dead-letter topic
	// and the stream moves on.
	Handler func(ctx context.Context, key json.RawMessage, event ChangeEvent) error

	// Header is one Kafka record header.
	Header struct {
		Key   string
		Value []byte
	}

	// Record is one Kafka record of a batch, with where it came from so a
	// consumer can make applying it idempotent (partition + offset are a
	// total order per partition, stable across redeliveries).
	//
	// Exactly one of these holds: Err is set (the value is not a change
	// event: a poison record), Event is nil (a tombstone: Debezium sends one
	// after every delete), or Event is the change.
	Record struct {
		Topic     string
		Partition int32
		Offset    int64
		Key       json.RawMessage
		Value     []byte
		Headers   []Header
		Timestamp time.Time
		Event     *ChangeEvent
		Err       error
	}

	// BatchHandler processes consecutive records of one partition, in
	// order. Return nil only once every record is durably handled (applied,
	// skipped as a duplicate, or dead-lettered): that is when their offsets
	// are committed. An error is retried with a capped backoff, blocking
	// the partition rather than skipping data, until it returns nil or the
	// service stops. ctx ends when the service stops.
	BatchHandler func(ctx context.Context, batch []Record) error

	// BatchOptions tunes SubscribeBatch. The zero value is usable.
	BatchOptions struct {
		// MaxBatch caps the records per handler call (DefaultMaxBatch if 0).
		MaxBatch int
		// OnAssigned and OnRevoked, when set, are told which partitions of
		// the topic this instance gained or lost in a consumer group
		// rebalance. They run on the client's goroutine: keep them quick.
		OnAssigned func(partitions []int32)
		OnRevoked  func(partitions []int32)
		// OnCommitError, when set, is told about an offset commit that
		// failed. The stream goes on and the records are delivered again
		// after a restart or rebalance; this is for logging and metrics.
		OnCommitError func(partition int32, offset int64, err error)
	}

	// ICDCService streams Debezium row changes from Kafka.
	ICDCService interface {
		// Run connects and starts every subscription. Stop waits for the
		// handlers running, then disconnects; changes not yet handled are
		// delivered again after the next Run, here or on another instance.
		Run(ctx context.Context) error
		Stop(ctx context.Context) error

		// Topic is the Debezium topic of schema.table:
		// <CDC_TOPIC_PREFIX>.<schema>.<table>.
		Topic(schema, table string) string

		// Subscribe delivers each change on topic to handler, at least once
		// and in order per row; it is committed only once handled or parked.
		// Tombstones (sent after a delete) are skipped. Before or after Run.
		Subscribe(ctx context.Context, topic string, handler Handler) error

		// SubscribeBatch delivers the changes on topic to handler in batches
		// of consecutive records of one partition, at least once and in
		// order per partition, with their partition and offset. Offsets are
		// committed only after the handler returns nil, and a failed commit
		// never stops the stream (the records are simply delivered again
		// after a restart or rebalance, which the handler must tolerate).
		// Rebalances are held off while a batch is being handled. Before or
		// after Run; a topic has either a Subscribe or a SubscribeBatch.
		SubscribeBatch(ctx context.Context, topic string, opts BatchOptions, handler BatchHandler) error

		// Unsubscribe stops delivering topic and leaves its consumer group,
		// waiting (until ctx ends) for the handler running now, whose context
		// is canceled. Changes it did not finish are delivered again to the
		// next consumer of the group. The service itself keeps running, so
		// other subscriptions are untouched. Unknown topics are ignored.
		Unsubscribe(ctx context.Context, topic string) error

		// DeadLetter publishes rec to its topic's dead-letter topic, with the
		// same headers Subscribe uses, in one attempt bounded by ctx. It is
		// for batch handlers that decide a record can never be handled.
		DeadLetter(ctx context.Context, rec Record, cause error) error

		// DeadLetters reads every record parked on topic's dead-letter topic, in
		// partition and offset order. Each keeps the original record's key, value
		// and headers (the dlq.* headers say where it came from and why it was
		// parked); Topic, Partition and Offset are those of the dead-letter
		// topic. A topic with no dead-letter topic yet has none: nil, nil. At most
		// limit records are returned (DefaultMaxDeadLetters if limit <= 0); a topic
		// holding more fails with ErrTooManyDeadLetters rather than being cut
		// short. It reads what is there when it starts and does not consume or
		// remove anything, so it can be called again. It needs no Run.
		DeadLetters(ctx context.Context, topic string, limit int) ([]Record, error)

		// GroupID is the consumer group prefix; the group of a topic is
		// GroupID()+"."+topic. A consumer can key its own checkpoints on it.
		GroupID() string
	}
)
