package broker

import (
	"encoding/json"
	"errors"
)

// Debezium operation codes (the envelope's "op").
const (
	OpCreate   = "c"
	OpUpdate   = "u"
	OpDelete   = "d"
	OpRead     = "r" // snapshot row
	OpTruncate = "t"
)

var ErrNotDebezium = errors.New("broker: value is not a Debezium change event")

// ChangeEvent is a Debezium change event as it arrives on a topic named
// <topic.prefix>.<schema>.<table>. Before is null for creates and snapshot
// reads, After is null for deletes. Use it inside a Subscribe handler:
//
//	ev, err := broker.DecodeChangeEvent(value)
type ChangeEvent struct {
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

// DecodeChangeEvent parses a Debezium JSON value, with or without the
// {"schema":…,"payload":…} wrapper. A nil result with no error is the
// tombstone Debezium sends after a delete; there is nothing to process.
// A value that is not a change event returns ErrNotDebezium.
func DecodeChangeEvent(value []byte) (*ChangeEvent, error) {
	if len(value) == 0 {
		return nil, nil
	}
	var wrapped struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(value, &wrapped); err != nil {
		return nil, errors.Join(ErrNotDebezium, err)
	}
	if len(wrapped.Payload) > 0 {
		value = wrapped.Payload
	}
	var ev ChangeEvent
	if err := json.Unmarshal(value, &ev); err != nil {
		return nil, errors.Join(ErrNotDebezium, err)
	}
	if ev.Op == "" {
		return nil, ErrNotDebezium
	}
	return &ev, nil
}
