package cqrs

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/uptrace/bun/schema"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// change is one row change of the stream, decoded and keyed.
type change[TData any] struct {
	op   cdc.ChangeType
	data TData // the row after the change; for a delete, the row that was removed
	key  string
	id   any
	lsn  int64 // where the change sits in the write database's WAL; 0 when the event does not say
}

// item is one record of a batch after decoding. Exactly one of change, dead
// or note explains it: change is written to the read database, dead can never
// be applied and is parked on the dead-letter topic, note is a record that is
// skipped on purpose (tombstone, truncate). Every item carries its record,
// because every record, handled or not, advances the checkpoint.
type item[TData any] struct {
	rec    cdc.Record
	change *change[TData]
	dead   error
	note   string
}

// prepare decodes a batch. It never fails as a whole: a record that cannot
// be decoded becomes a dead item, so one poison record cannot stop the rest.
func (m *model[TData]) prepare(batch []cdc.Record) []item[TData] {
	out := make([]item[TData], len(batch))
	for i := range batch {
		out[i] = m.decodeItem(batch[i])
	}
	return out
}

func (m *model[TData]) decodeItem(rec cdc.Record) item[TData] {
	it := item[TData]{rec: rec}
	switch {
	case rec.Err != nil:
		it.dead = rec.Err
		return it
	case rec.Event == nil:
		it.note = noteTombstone
		return it
	}
	ev := rec.Event
	if t := ev.Source.Table; t != "" && t != m.name {
		it.dead = fmt.Errorf("%w: event is for table %q, this service replicates %q", ErrMalformedChange, t, m.name)
		return it
	}
	var (
		data TData
		err  error
	)
	switch ev.Op {
	case cdc.OpCreate, cdc.OpUpdate, cdc.OpRead:
		data, err = m.decodeRow(ev.After)
	case cdc.OpDelete:
		data, err = m.decodeDeleted(rec.Key, ev.Before)
	case cdc.OpTruncate:
		it.note = noteTruncate
		return it
	default:
		it.note = noteOther
		return it
	}
	if err != nil {
		it.dead = err
		return it
	}
	key, id, err := m.identify(&data)
	if err != nil {
		it.dead = err
		return it
	}
	it.change = &change[TData]{op: ev.Op, data: data, key: key, id: id, lsn: ev.Source.LSN}
	return it
}

// decodeDeleted is the row a delete removed: the full old row when Debezium
// sent it (REPLICA IDENTITY FULL), so a change hook can describe what was
// deleted, else the row's key, which is all a delete needs.
func (m *model[TData]) decodeDeleted(key, before json.RawMessage) (TData, error) {
	if row, err := m.decodeRow(before); err == nil {
		if _, _, err := m.identify(&row); err == nil {
			return row, nil
		}
	}
	return m.decodeRow(key)
}

// decodeRow turns the JSON object Debezium sends for a row into a TData.
//
// Columns the model does not have are ignored and columns the event does not
// have stay zero, so a table that gained or lost a column does not break
// replication while the two databases are migrated one after the other.
func (m *model[TData]) decodeRow(raw json.RawMessage) (TData, error) {
	var out TData
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return out, fmt.Errorf("%w: the row is null", ErrMalformedChange)
	}
	var cols map[string]json.RawMessage
	if err := json.Unmarshal(raw, &cols); err != nil {
		return out, fmt.Errorf("%w: the row is not a JSON object: %v", ErrMalformedChange, err)
	}
	if inner, ok := cols["payload"]; ok && m.fields["payload"] == nil && len(cols) <= 2 {
		return m.decodeRow(inner) // a key or value wrapped with its schema
	}
	rv := reflect.ValueOf(&out).Elem()
	for name, val := range cols {
		f := m.fields[name]
		if f == nil {
			continue
		}
		src, err := toSource(f, val)
		if err != nil {
			return out, fmt.Errorf("%w: column %q: %w", ErrMalformedChange, name, err)
		}
		if err := f.ScanValue(rv, src); err != nil {
			// The scanner's own text can quote the value; the type is enough to debug.
			return out, fmt.Errorf("%w: column %q does not fit a %s", ErrMalformedChange, name, f.IndirectType)
		}
	}
	return out, nil
}

// toSource converts one JSON value of a Debezium row into what bun's scanner
// for field f accepts: nil, string, int64, float64, bool or []byte.
func toSource(f *schema.Field, raw json.RawMessage) (any, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty value")
	}
	t := f.IndirectType
	switch raw[0] {
	case 'n':
		return nil, nil
	case 't', 'f':
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return nil, err
		}
		return b, nil
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		if s == unavailableValue {
			return nil, ErrUnavailableValue
		}
		if strings.IndexByte(s, 0) >= 0 {
			return nil, fmt.Errorf("text contains a NUL byte, which Postgres cannot store")
		}
		if strings.Contains(strings.ToLower(f.CreateTableSQLType), "json") {
			return s, nil // json and jsonb arrive as JSON text in a string, even for a []byte field
		}
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			b, err := base64.StdEncoding.DecodeString(s) // bytea reaches us base64-encoded
			if err != nil {
				return nil, fmt.Errorf("bytea is not base64: %v", err)
			}
			return b, nil
		}
		return s, nil
	case '{', '[':
		if !isJSONColumn(f) {
			return nil, fmt.Errorf("an object or array for a %s column", f.IndirectType)
		}
		return append([]byte(nil), raw...), nil // json and jsonb
	}
	return numberSource(f, raw)
}

// isJSONColumn reports whether the field holds a JSON document: a jsonb or json
// column, or a Go type that is a document (a map, a struct, a slice that is not
// bytes) rather than a scalar.
func isJSONColumn(f *schema.Field) bool {
	if strings.Contains(strings.ToLower(f.CreateTableSQLType), "json") {
		return true
	}
	t := f.IndirectType
	switch t.Kind() {
	case reflect.Map, reflect.Struct, reflect.Interface:
		return t != timeType
	case reflect.Slice:
		return t.Elem().Kind() != reflect.Uint8
	}
	return false
}

// fitsInteger reports whether n can be held by the integer kind k without
// wrapping around. bun's scanners convert with SetInt/SetUint, which silently
// truncate: 40000 in an int16 would become -25536 and the read model would
// quietly hold a number that never existed.
func fitsInteger(k reflect.Kind, n int64) bool {
	switch k {
	case reflect.Int8:
		return n >= math.MinInt8 && n <= math.MaxInt8
	case reflect.Int16:
		return n >= math.MinInt16 && n <= math.MaxInt16
	case reflect.Int32:
		return n >= math.MinInt32 && n <= math.MaxInt32
	case reflect.Uint8:
		return n >= 0 && n <= math.MaxUint8
	case reflect.Uint16:
		return n >= 0 && n <= math.MaxUint16
	case reflect.Uint32:
		return n >= 0 && n <= math.MaxUint32
	case reflect.Uint, reflect.Uint64, reflect.Uintptr:
		return n >= 0
	}
	return true // int, int64: every int64 fits
}

// numberSource converts a JSON number. Integers stay integers (exact up to
// 64 bits); a float column is given a float64 because bun's float scanner
// refuses an int64; a date column arrives as days since the epoch.
func numberSource(f *schema.Field, raw json.RawMessage) (any, error) {
	text := string(raw)
	t := f.IndirectType
	n, intErr := strconv.ParseInt(text, 10, 64)
	if t == timeType {
		if intErr == nil && strings.EqualFold(f.CreateTableSQLType, "date") {
			return time.Unix(n*86400, 0).UTC(), nil
		}
		return nil, fmt.Errorf("a number for a %s column (a timestamp must be timestamptz)", f.CreateTableSQLType)
	}
	if t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64 {
		x, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, fmt.Errorf("not a number")
		}
		return x, nil
	}
	if intErr == nil {
		if !fitsInteger(t.Kind(), n) {
			return nil, fmt.Errorf("a number outside the range of a %s", t)
		}
		return n, nil
	}
	// Not an int64: either a number too large for one (a uint64 above the int64
	// range is the only legitimate case) or not an integer at all.
	if t.Kind() == reflect.Uint64 || t.Kind() == reflect.Uint {
		if u, err := strconv.ParseUint(text, 10, 64); err == nil {
			return u, nil
		}
	}
	if isIntegerKind(t.Kind()) {
		return nil, fmt.Errorf("not an integer that fits a %s", t)
	}
	x, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return nil, fmt.Errorf("not a number")
	}
	return x, nil
}

func isIntegerKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return true
	}
	return false
}
