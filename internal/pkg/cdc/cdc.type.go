package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Debezium operation codes (the envelope's "op").
const (
	OpCreate   = "c"
	OpUpdate   = "u"
	OpDelete   = "d"
	OpRead     = "r" // a row read during the initial snapshot
	OpTruncate = "t"
)

// Delivery settings.
const (
	defaultTopicPrefix = "ecoop"
	defaultDLQSuffix   = ".dlq"
	defaultAttempts    = 3
	defaultBackoff     = 200 * time.Millisecond
	maxDLQBackoff      = 5 * time.Second // cap while the DLQ itself is unreachable
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
)

type (
	// ChangeEvent is one Debezium row change. Before is null for creates and
	// snapshot reads; After is null for deletes. With Postgres's default
	// replica identity, Before holds only the primary key on updates and
	// deletes.
	ChangeEvent struct {
		Op     string          `json:"op"`
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

	// CDCServices streams Debezium row changes from Kafka.
	CDCServices interface {
		// Run connects and starts every subscription. Stop waits for the
		// handlers running, then disconnects; changes not yet handled are
		// delivered again after the next Run, here or on another instance.
		Run(ctx context.Context) error
		Stop(ctx context.Context) error

		// Topic is the Debezium topic of schema.table:
		// <DEBEZIUM_TOPIC_PREFIX>.<schema>.<table>.
		Topic(schema, table string) string

		// Subscribe delivers each change on topic to handler, at least once
		// and in order per row; it is committed only once handled or parked.
		// Tombstones (sent after a delete) are skipped. Before or after Run.
		Subscribe(ctx context.Context, topic string, handler Handler) error
	}
)
