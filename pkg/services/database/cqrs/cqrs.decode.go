package cqrs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"
)

// Outbox messages arrive in one of two formats:
//
//   - the native envelope {event_id, change_type, payload}, published by this service or by tests;
//   - a Debezium change event (Postgres connector, JsonConverter, schemas enabled or not):
//     {before, after, source{db, schema, table, lsn, txId}, op}.
//
// Debezium rows are keyed by column name and encoded by Debezium's rules, so they are mapped onto
// TData by bun column name (never by json tag, which belongs to the API) with these conversions:
//   - jsonb arrives as a JSON string and is decoded into the map/struct/slice field;
//   - bytea arrives as base64 and is decoded into []byte;
//   - timestamptz arrives as an ISO-8601 string; timestamp and date arrive as integers (days, millis,
//     micros or nanos since the epoch, told apart by magnitude) and are converted to time.Time;
//   - numeric needs the connector option decimal.handling.mode=string (the default "precise" mode
//     sends base64 bytes that cannot be decoded without the schema); money in integer minor units
//     (bigint) needs nothing.
//
// Columns without a matching field are ignored, so the writer can gain columns before the reader's
// model does.

var (
	ErrMalformedMessage   = errors.New("cqrs: malformed outbox message")
	ErrUnsupportedChange  = errors.New("cqrs: unsupported change operation")
	errDebeziumNoRowImage = errors.New("debezium event has no row image")
)

type debeziumEvent struct {
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
	Op     string          `json:"op"`
	Source struct {
		DB     string          `json:"db"`
		Schema string          `json:"schema"`
		Table  string          `json:"table"`
		LSN    json.RawMessage `json:"lsn"`
		TxID   json.RawMessage `json:"txId"`
	} `json:"source"`
}

// decodeMessage turns one broker record into a queue payload.
func (c *CQRSService[TData, TResponse, TRequest, TID]) decodeMessage(key, value []byte) (CQRSQueuePayload[TData], error) {
	var zero CQRSQueuePayload[TData]
	value = bytes.TrimSpace(value)
	// encoding/json would silently turn invalid UTF-8 into U+FFFD: refuse it instead of storing a
	// corrupted copy on the read model.
	if !utf8.Valid(value) {
		return zero, fmt.Errorf("%w: not valid UTF-8", ErrMalformedMessage)
	}
	if len(value) == 0 || value[0] != '{' {
		return zero, fmt.Errorf("%w: not a JSON object", ErrMalformedMessage)
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(value, &probe); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrMalformedMessage, err)
	}
	// Debezium with schemas enabled wraps the event: {"schema": ..., "payload": {...}}.
	if _, hasSchema := probe["schema"]; hasSchema {
		if inner, ok := probe["payload"]; ok {
			value = inner
			probe = nil
			if err := json.Unmarshal(value, &probe); err != nil {
				return zero, fmt.Errorf("%w: %w", ErrMalformedMessage, err)
			}
		}
	}
	if _, isDebezium := probe["op"]; isDebezium {
		return c.decodeDebezium(key, value)
	}
	var env CQRSQueuePayload[TData]
	if err := json.Unmarshal(value, &env); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrMalformedMessage, err)
	}
	if env.EventID == "" {
		if len(key) == 0 {
			return zero, fmt.Errorf("%w: no event id and no key", ErrMalformedMessage)
		}
		env.EventID = string(key)
	}
	return env, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) decodeDebezium(key, value []byte) (CQRSQueuePayload[TData], error) {
	var zero CQRSQueuePayload[TData]
	var ev debeziumEvent
	if err := json.Unmarshal(value, &ev); err != nil {
		return zero, fmt.Errorf("%w: debezium: %w", ErrMalformedMessage, err)
	}
	var change ChangeType
	row := ev.After
	switch ev.Op {
	case "c", "r":
		change = ChangeTypeCreated
	case "u":
		change = ChangeTypeUpdated
	case "d":
		change, row = ChangeTypeDeleted, ev.Before
	default: // "t" (truncate), "m" (logical message)
		return zero, fmt.Errorf("%w: debezium op %q", ErrUnsupportedChange, ev.Op)
	}
	if len(row) == 0 || string(row) == "null" {
		return zero, fmt.Errorf("%w: %w (op %q)", ErrMalformedMessage, errDebeziumNoRowImage, ev.Op)
	}
	payload, err := c.debeziumRow(row)
	if err != nil {
		return zero, err
	}
	// Deterministic, so a redelivered event is recognised: the LSN is unique per row change, and the
	// record key separates the rows of a snapshot ("r"), which all share the snapshot's LSN.
	eventID := fmt.Sprintf("dbz:%s.%s:%s:%s:%s:%s", ev.Source.Schema, ev.Source.Table,
		bytes.Trim(ev.Source.LSN, `"`), bytes.Trim(ev.Source.TxID, `"`), ev.Op, compactKey(key))
	return CQRSQueuePayload[TData]{EventID: eventID, ChangeType: change, Payload: payload}, nil
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) debeziumRow(raw json.RawMessage) (TData, error) {
	var out TData
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // keep bigint values exact
	var cols map[string]any
	if err := dec.Decode(&cols); err != nil {
		return out, fmt.Errorf("%w: debezium row: %w", ErrMalformedMessage, err)
	}
	v := reflect.ValueOf(&out).Elem()
	for col, val := range cols {
		idx, ok := c.columnFields[col]
		if !ok || val == nil {
			continue
		}
		f := v.Field(idx)
		if err := setDebeziumField(f, val); err != nil {
			return out, fmt.Errorf("%w: column %s: %w", ErrMalformedMessage, col, err)
		}
	}
	return out, nil
}

var timeType = reflect.TypeFor[time.Time]()

func setDebeziumField(f reflect.Value, val any) error {
	target := f.Type()
	base := target
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	switch {
	case base == timeType:
		t, err := debeziumTime(val)
		if err != nil {
			return err
		}
		return assign(f, reflect.ValueOf(t))
	case base.Kind() == reflect.Map || base.Kind() == reflect.Struct ||
		(base.Kind() == reflect.Slice && base.Elem().Kind() != reflect.Uint8):
		if s, ok := val.(string); ok { // jsonb / json columns arrive as a JSON string
			ptr := reflect.New(base)
			if err := json.Unmarshal([]byte(s), ptr.Interface()); err != nil {
				return err
			}
			return assign(f, ptr.Elem())
		}
	}
	b, err := json.Marshal(val)
	if err != nil {
		return err
	}
	ptr := reflect.New(target)
	if err := json.Unmarshal(b, ptr.Interface()); err != nil {
		return err
	}
	f.Set(ptr.Elem())
	return nil
}

func assign(f reflect.Value, v reflect.Value) error {
	if f.Kind() == reflect.Pointer {
		p := reflect.New(f.Type().Elem())
		p.Elem().Set(v)
		f.Set(p)
		return nil
	}
	f.Set(v)
	return nil
}

// debeziumTime decodes Debezium's time encodings: ISO-8601 strings (timestamptz, ZonedTimestamp) and
// integers since the epoch whose unit depends on the column type (date: days; timestamp(0-3): millis;
// timestamp(4-6): micros; nanos for NanoTimestamp). The unit is inferred from the magnitude, which is
// unambiguous for any instant between 1970 and 2200.
func debeziumTime(val any) (time.Time, error) {
	switch x := val.(type) {
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02"} {
			if t, err := time.Parse(layout, x); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("unrecognised time %q", x)
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return time.Time{}, err
		}
		abs := n
		if abs < 0 {
			abs = -abs
		}
		switch {
		case abs < 1e7:
			return time.Unix(0, 0).UTC().AddDate(0, 0, int(n)), nil
		case abs < 1e13:
			return time.UnixMilli(n).UTC(), nil
		case abs < 1e16:
			return time.UnixMicro(n).UTC(), nil
		default:
			return time.Unix(0, n).UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time value %T", val)
}

// columnFieldIndexes maps every bun column name of TData to its struct field index.
func columnFieldIndexes[TData any]() map[string]int {
	t := reflect.TypeFor[TData]()
	out := map[string]int{}
	if t.Kind() != reflect.Struct {
		return out
	}
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, _, _ := strings.Cut(f.Tag.Get("bun"), ",")
		if name == "" || name == "-" {
			continue
		}
		out[name] = i
	}
	return out
}

func compactKey(key []byte) string {
	var buf bytes.Buffer
	if json.Compact(&buf, key) == nil {
		return buf.String()
	}
	return string(key)
}

// DecodeChange decodes one outbox message (native envelope or Debezium change event) for TData,
// exactly as the runner does. It touches no database, so it suits validation and fuzzing.
func DecodeChange[TData any](key, value []byte) (CQRSQueuePayload[TData], error) {
	c := &CQRSService[TData, struct{}, struct{}, string]{columnFields: columnFieldIndexes[TData]()}
	return c.decodeMessage(key, value)
}
