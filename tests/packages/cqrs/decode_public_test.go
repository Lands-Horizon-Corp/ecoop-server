//go:build integration

package cqrs_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/uptrace/bun"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// How a Debezium row becomes a row of the read database, tested only through the
// public API: events go through a running service, and what is checked is what
// lands in the read database, or that the record was parked. Nothing here reaches
// into unexported code.

// Kitchen is a model with one column of every type the replicator handles.
type Kitchen struct {
	bun.BaseModel `bun:"table:kitchen"`

	ID    int64           `bun:"id,pk"`
	S     string          `bun:"s"`
	SP    *string         `bun:"sp"`
	I16   int16           `bun:"i16"`
	I32   int32           `bun:"i32"`
	I64   int64           `bun:"i64"`
	U8    uint8           `bun:"u8"`
	U32   uint32          `bun:"u32"`
	F32   float32         `bun:"f32"`
	F64   float64         `bun:"f64"`
	B     bool            `bun:"b"`
	BP    *bool           `bun:"bp"`
	T     time.Time       `bun:"t,type:timestamptz"`
	TP    *time.Time      `bun:"tp,type:timestamptz"`
	D     time.Time       `bun:"d,type:date"`
	J     json.RawMessage `bun:"j,type:jsonb"`
	Bytes []byte          `bun:"bytes"`
	IP    *int64          `bun:"ip"`
}

const kitchenDDL = `CREATE TABLE kitchen (
	id    bigint PRIMARY KEY,
	s     text,
	sp    text,
	i16   smallint,
	i32   integer,
	i64   bigint,
	u8    smallint,
	u32   bigint,
	f32   real,
	f64   double precision,
	b     boolean,
	bp    boolean,
	t     timestamptz,
	tp    timestamptz,
	d     date,
	j     jsonb,
	bytes bytea,
	ip    bigint
)`

const kitchenTopic = "test.public.kitchen"

// kitchenRig is a pair of databases with the kitchen table on both sides and a
// service replicating it.
type kitchenRig struct {
	t   testing.TB
	p   *pair
	f   *fakeCDC
	svc cqrs.ICQRSService[Kitchen, AccountResponse, struct{}, int64]
	off int64
}

func newKitchen(t testing.TB) *kitchenRig {
	t.Helper()
	p := newPair(t, 6)
	p.exec(p.write, kitchenDDL)
	p.exec(p.write, `ALTER TABLE kitchen REPLICA IDENTITY FULL`)
	p.exec(p.read, kitchenDDL)
	f := newFakeCDC(fmt.Sprintf("kitchen-%d", seqNo.Add(1)))
	svc := newTyped[Kitchen](t, p, f, "", func(c *cqrs.CQRSService[Kitchen, AccountResponse, struct{}, int64]) {
		c.Topic = kitchenTopic
		c.RepairInterval = -1 // the parking itself is under test, not the repair of it
	})
	start(t, svc)
	return &kitchenRig{t: t, p: p, f: f, svc: svc}
}

// event is a Debezium record of the kitchen table at the next offset.
func (k *kitchenRig) event(op cdc.ChangeType, key, before, after string) cdc.Record {
	ev := &cdc.ChangeEvent{Op: op, Before: json.RawMessage(before), After: json.RawMessage(after)}
	ev.Source.Table = "kitchen"
	r := cdc.Record{
		Topic: kitchenTopic, Partition: 0, Offset: k.off, Key: json.RawMessage(key), Event: ev,
		Timestamp: baseTime.Add(time.Duration(k.off) * time.Millisecond),
	}
	k.off++
	return r
}

// send delivers one record and waits until it is handled (applied, skipped or parked).
func (k *kitchenRig) send(rec cdc.Record) {
	k.t.Helper()
	if err := k.f.deliver(ctxTimeout(k.t, time.Minute), []cdc.Record{rec}); err != nil {
		k.t.Fatal(err)
	}
}

func (k *kitchenRig) parked() int { return len(k.f.deadLettered()) }

// row is the stored kitchen row with id, or false when there is none.
func (k *kitchenRig) row(id int64) (Kitchen, bool) {
	k.t.Helper()
	var out Kitchen
	err := k.p.read.Client().NewSelect().Model(&out).Where("id = ?", id).Scan(context.Background())
	if errors.Is(err, sql.ErrNoRows) {
		return Kitchen{}, false
	}
	if err != nil {
		k.t.Fatalf("reading kitchen %d: %v", id, err)
	}
	return out, true
}

func (k *kitchenRig) count() int { return k.p.count(k.p.read, "kitchen") }

// ---- values that must arrive exactly ---------------------------------------

func TestDecodePublic_ValidValuesArriveExactly(t *testing.T) {
	t.Parallel()
	ok := []struct {
		name  string
		row   string
		check func(k Kitchen) bool
	}{
		{"string", `{"id":1,"s":"hello"}`, func(k Kitchen) bool { return k.S == "hello" }},
		{"empty string", `{"id":1,"s":""}`, func(k Kitchen) bool { return k.S == "" }},
		{"unicode", `{"id":1,"s":"日本語 🎉 ñ"}`, func(k Kitchen) bool { return k.S == "日本語 🎉 ñ" }},
		{"escapes", `{"id":1,"s":"a\"b\\c\nd\te"}`, func(k Kitchen) bool { return k.S == "a\"b\\c\nd\te" }},
		{"quotes and sql", `{"id":1,"s":"it's \"x\"; -- ? % _ \\"}`, func(k Kitchen) bool { return k.S == `it's "x"; -- ? % _ \` }},
		{"long string", `{"id":1,"s":"` + strings.Repeat("x", 100000) + `"}`, func(k Kitchen) bool { return len(k.S) == 100000 }},
		{"pointer string set", `{"id":1,"sp":"x"}`, func(k Kitchen) bool { return k.SP != nil && *k.SP == "x" }},
		{"pointer string null", `{"id":1,"sp":null}`, func(k Kitchen) bool { return k.SP == nil }},
		{"pointer string empty is not null", `{"id":1,"sp":""}`, func(k Kitchen) bool { return k.SP != nil && *k.SP == "" }},
		{"pointer string absent", `{"id":1}`, func(k Kitchen) bool { return k.SP == nil }},
		{"int16 max", `{"id":1,"i16":32767}`, func(k Kitchen) bool { return k.I16 == 32767 }},
		{"int16 min", `{"id":1,"i16":-32768}`, func(k Kitchen) bool { return k.I16 == -32768 }},
		{"int32 max", `{"id":1,"i32":2147483647}`, func(k Kitchen) bool { return k.I32 == math.MaxInt32 }},
		{"int32 min", `{"id":1,"i32":-2147483648}`, func(k Kitchen) bool { return k.I32 == math.MinInt32 }},
		{"int64 max", `{"id":1,"i64":9223372036854775807}`, func(k Kitchen) bool { return k.I64 == math.MaxInt64 }},
		{"int64 min", `{"id":1,"i64":-9223372036854775808}`, func(k Kitchen) bool { return k.I64 == math.MinInt64 }},
		{"int zero", `{"id":1,"i64":0}`, func(k Kitchen) bool { return k.I64 == 0 }},
		{"uint8 max", `{"id":1,"u8":255}`, func(k Kitchen) bool { return k.U8 == 255 }},
		{"uint32 max", `{"id":1,"u32":4294967295}`, func(k Kitchen) bool { return k.U32 == math.MaxUint32 }},
		{"int sent as a string", `{"id":1,"i64":"123"}`, func(k Kitchen) bool { return k.I64 == 123 }},
		{"float32", `{"id":1,"f32":1.5}`, func(k Kitchen) bool { return k.F32 == 1.5 }},
		{"float64 pi", `{"id":1,"f64":3.141592653589793}`, func(k Kitchen) bool { return k.F64 == math.Pi }},
		{"float from an integer", `{"id":1,"f64":3}`, func(k Kitchen) bool { return k.F64 == 3 }},
		{"float exponent", `{"id":1,"f64":1.5e3}`, func(k Kitchen) bool { return k.F64 == 1500 }},
		{"float negative", `{"id":1,"f64":-0.25}`, func(k Kitchen) bool { return k.F64 == -0.25 }},
		{"float sent as a string", `{"id":1,"f64":"2.5"}`, func(k Kitchen) bool { return k.F64 == 2.5 }},
		{"bool true", `{"id":1,"b":true}`, func(k Kitchen) bool { return k.B }},
		{"bool false", `{"id":1,"b":false}`, func(k Kitchen) bool { return !k.B }},
		{"bool pointer false is not null", `{"id":1,"bp":false}`, func(k Kitchen) bool { return k.BP != nil && !*k.BP }},
		{"bool pointer null", `{"id":1,"bp":null}`, func(k Kitchen) bool { return k.BP == nil }},
		{"timestamptz utc", `{"id":1,"t":"2026-01-02T03:04:05Z"}`, func(k Kitchen) bool { return k.T.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) }},
		{"timestamptz microseconds", `{"id":1,"t":"2026-01-02T03:04:05.123456Z"}`, func(k Kitchen) bool { return k.T.Nanosecond() == 123456000 }},
		{"timestamptz with an offset", `{"id":1,"t":"2026-01-02T11:04:05+08:00"}`, func(k Kitchen) bool { return k.T.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) }},
		{"timestamptz null pointer", `{"id":1,"tp":null}`, func(k Kitchen) bool { return k.TP == nil }},
		{"timestamptz pointer set", `{"id":1,"tp":"2026-05-05T00:00:00Z"}`, func(k Kitchen) bool { return k.TP != nil && k.TP.Year() == 2026 }},
		{"date as days since the epoch", `{"id":1,"d":20455}`, func(k Kitchen) bool { return k.D.Equal(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)) }},
		{"date epoch", `{"id":1,"d":0}`, func(k Kitchen) bool { return k.D.Equal(time.Unix(0, 0).UTC()) }},
		{"date before the epoch", `{"id":1,"d":-1}`, func(k Kitchen) bool { return k.D.Year() == 1969 }},
		{"jsonb object", `{"id":1,"j":"{\"a\":[1,2,{\"b\":null}]}"}`, func(k Kitchen) bool { return jsonEqual(k.J, `{"a":[1,2,{"b":null}]}`) }},
		{"jsonb array", `{"id":1,"j":"[1,2,3]"}`, func(k Kitchen) bool { return jsonEqual(k.J, `[1,2,3]`) }},
		{"jsonb scalar", `{"id":1,"j":"42"}`, func(k Kitchen) bool { return jsonEqual(k.J, `42`) }},
		{"bytea", `{"id":1,"bytes":"AQIDBA=="}`, func(k Kitchen) bool { return string(k.Bytes) == "\x01\x02\x03\x04" }},
		{"bytea empty", `{"id":1,"bytes":""}`, func(k Kitchen) bool { return len(k.Bytes) == 0 }},
		{"int pointer", `{"id":1,"ip":7}`, func(k Kitchen) bool { return k.IP != nil && *k.IP == 7 }},
		{"int pointer null", `{"id":1,"ip":null}`, func(k Kitchen) bool { return k.IP == nil }},
		{"unmapped columns are ignored", `{"id":1,"hidden":"x","nope":1,"s":"kept"}`, func(k Kitchen) bool { return k.S == "kept" }},
		{"null for every column", `{"id":1,"s":null,"i64":null,"f64":null,"b":null,"t":null,"d":null,"bytes":null}`, func(k Kitchen) bool { return k.S == "" && k.I64 == 0 && !k.B }},
		{"whitespace around values", "{ \"id\":1 , \"s\" : \"a\" }", func(k Kitchen) bool { return k.S == "a" }},
		{"key order is irrelevant", `{"s":"z","i64":5,"id":1}`, func(k Kitchen) bool { return k.S == "z" && k.I64 == 5 }},
		{"id sent as a string", `{"id":"1"}`, func(k Kitchen) bool { return k.ID == 1 }},
		{"id beyond 2^53", `{"id":9007199254740993}`, func(k Kitchen) bool { return k.ID == 9007199254740993 }},
	}
	// One database pair for all of them: each case uses a row of its own.
	rig := newKitchen(t)
	for i, tc := range ok {
		t.Run(tc.name, func(t *testing.T) {
			id := int64(1)
			row := tc.row
			// Give every case its own key so they do not overwrite each other.
			if strings.Contains(row, `"id":"1"`) {
				id = 1
			} else if strings.Contains(row, `"id":9007199254740993`) {
				id = 9007199254740993
			} else {
				id = int64(1000 + i)
				if !strings.Contains(row, `"id":1`) {
					t.Fatalf("test case %q must spell its key as \"id\":1 so the harness can give it its own row", tc.name)
				}
				row = strings.Replace(row, `"id":1`, fmt.Sprintf(`"id":%d`, id), 1)
			}
			key := fmt.Sprintf(`{"id":%d}`, id)
			before := rig.parked()
			rig.send(rig.event(cdc.OpCreate, key, `null`, row))
			if got := rig.parked(); got != before {
				t.Fatalf("a valid row was parked as poison: %s", row)
			}
			k, found := rig.row(id)
			if !found {
				t.Fatalf("the row never arrived: %s", row)
			}
			if !tc.check(k) {
				t.Fatalf("stored %+v from %s", k, row)
			}
		})
	}
}

func jsonEqual(raw json.RawMessage, want string) bool {
	var a, b any
	if json.Unmarshal(raw, &a) != nil || json.Unmarshal([]byte(want), &b) != nil {
		return false
	}
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// ---- values that must be refused -------------------------------------------

func TestDecodePublic_BadRowsAreParkedAndNeverStored(t *testing.T) {
	t.Parallel()
	bad := map[string]string{
		"row is null":                `null`,
		"row is empty":               ``,
		"row is a string":            `"x"`,
		"row is a number":            `5`,
		"row is an array":            `[1]`,
		"row is truncated":           `{"id":1,"s":`,
		"row is not json":            `{id:1}`,
		"no id":                      `{"s":"x"}`,
		"zero id":                    `{"id":0}`,
		"null id":                    `{"id":null}`,
		"string for int":             `{"id":1,"i64":"many"}`,
		"object for int":             `{"id":1,"i64":{}}`,
		"array for int":              `{"id":1,"i64":[1]}`,
		"bool for int":               `{"id":1,"i64":true}`,
		"fraction for int":           `{"id":1,"i64":1.5}`,
		"int too big for int64":      `{"id":1,"i64":9223372036854775808}`,
		"int too small for int64":    `{"id":1,"i64":-9223372036854775809}`,
		"object for a string column": `{"id":1,"s":{"a":1}}`,
		"array for a string column":  `{"id":1,"s":[1,2]}`,
		"object for time":            `{"id":1,"t":{}}`,
		"number for timestamptz":     `{"id":1,"t":1700000000000}`,
		"garbage for time":           `{"id":1,"t":"yesterday"}`,
		"garbage for bool":           `{"id":1,"b":"perhaps"}`,
		"bytea not base64":           `{"id":1,"bytes":"!!!"}`,
		"toast placeholder":          `{"id":1,"s":"__debezium_unavailable_value"}`,
		"toast placeholder in json":  `{"id":1,"j":"__debezium_unavailable_value"}`,
		"nul byte in text":           `{"id":1,"s":"a\u0000b"}`,
		"nul byte in pointer text":   `{"id":1,"sp":"\u0000"}`,
		"unavailable for int":        `{"id":1,"i64":"__debezium_unavailable_value"}`,
		"float for date":             `{"id":1,"d":1.5}`,
		"text for date":              `{"id":1,"d":"not a date"}`,
	}
	rig := newKitchen(t)
	i := 0
	for name, row := range bad {
		i++
		t.Run(name, func(t *testing.T) {
			before, stored := rig.parked(), rig.count()
			rig.send(rig.event(cdc.OpCreate, `{"id":1}`, `null`, row))
			if rig.parked() != before+1 {
				t.Fatalf("%q was not parked (parked %d -> %d)", row, before, rig.parked())
			}
			if rig.count() != stored {
				t.Fatalf("%q reached the read database", row)
			}
		})
	}
}

// A value that does not fit the Go type is refused, never silently truncated: a
// 40000 in an int16 column would otherwise become -25536.
func TestDecodePublic_SizedIntegersAreNeverTruncated(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, row string }{
		{"int16 over", `{"id":1,"i16":40000}`},
		{"int16 under", `{"id":1,"i16":-40000}`},
		{"int32 over", `{"id":1,"i32":3000000000}`},
		{"int32 under", `{"id":1,"i32":-3000000000}`},
		{"uint8 over", `{"id":1,"u8":256}`},
		{"uint8 negative", `{"id":1,"u8":-1}`},
		{"uint32 over", `{"id":1,"u32":4294967296}`},
		{"uint32 negative", `{"id":1,"u32":-5}`},
	}
	rig := newKitchen(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := rig.parked()
			rig.send(rig.event(cdc.OpCreate, `{"id":1}`, `null`, tc.row))
			if rig.parked() != before+1 {
				t.Fatalf("%s was accepted", tc.row)
			}
			if _, found := rig.row(1); found {
				t.Fatalf("%s was stored (truncated)", tc.row)
			}
		})
	}
}

// ---- kinds of event --------------------------------------------------------

func TestDecodePublic_EventKinds(t *testing.T) {
	t.Parallel()
	row := `{"id":4,"s":"x"}`
	cases := []struct {
		name       string
		op         cdc.ChangeType
		key        string
		before     string
		after      string
		wantStored bool // the row exists afterwards
		wantParked bool
	}{
		{"create", cdc.OpCreate, `{"id":4}`, `null`, row, true, false},
		{"update", cdc.OpUpdate, `{"id":4}`, `null`, row, true, false},
		{"snapshot read", cdc.OpRead, `{"id":4}`, `null`, row, true, false},
		{"truncate is skipped", cdc.OpTruncate, `null`, `null`, `null`, false, false},
		{"message is skipped", "m", `null`, `null`, `null`, false, false},
		{"unknown operation is skipped", "z", `null`, `null`, `null`, false, false},
		{"update with a null after is poison", cdc.OpUpdate, `{"id":4}`, `null`, `null`, false, true},
		{"create with only a before is poison", cdc.OpCreate, `{"id":4}`, row, `null`, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newKitchen(t)
			rig.send(rig.event(tc.op, tc.key, tc.before, tc.after))
			if got := rig.parked() == 1; got != tc.wantParked {
				t.Fatalf("parked = %v, want %v", got, tc.wantParked)
			}
			if _, found := rig.row(4); found != tc.wantStored {
				t.Fatalf("row stored = %v, want %v", found, tc.wantStored)
			}
		})
	}
}

// The three shapes of a delete: the full old row, a key-only before, and no before.
func TestDecodePublic_DeleteShapes(t *testing.T) {
	t.Parallel()
	row := `{"id":4,"s":"x"}`
	cases := []struct {
		name, key, before string
		wantParked        bool
	}{
		{"with the full old row", `{"id":4}`, row, false},
		{"key only, before null", `{"id":4}`, `null`, false},
		{"before holds only the key", `{"id":4}`, `{"id":4}`, false},
		{"no key and no before is poison", `null`, `null`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newKitchen(t)
			rig.p.exec(rig.p.read, `INSERT INTO kitchen (id, s) VALUES (4, 'there')`)
			rig.send(rig.event(cdc.OpDelete, tc.key, tc.before, `null`))
			if got := rig.parked() == 1; got != tc.wantParked {
				t.Fatalf("parked = %v, want %v", got, tc.wantParked)
			}
			_, found := rig.row(4)
			if found == !tc.wantParked {
				t.Fatalf("row present = %v after a delete (parked=%v)", found, tc.wantParked)
			}
		})
	}
}

func TestDecodePublic_TombstoneIsSkippedQuietly(t *testing.T) {
	t.Parallel()
	rig := newKitchen(t)
	rig.p.exec(rig.p.read, `INSERT INTO kitchen (id, s) VALUES (4, 'kept')`)
	tomb := cdc.Record{Topic: kitchenTopic, Partition: 0, Offset: rig.off, Key: json.RawMessage(`{"id":4}`)}
	rig.off++
	rig.send(tomb)
	if rig.parked() != 0 {
		t.Fatal("a tombstone is not poison")
	}
	if k, found := rig.row(4); !found || k.S != "kept" {
		t.Fatalf("a tombstone must not change data: %+v %v", k, found)
	}
	if got := rig.svc.Stats().Tombstones; got != 1 {
		t.Fatalf("Stats.Tombstones = %d", got)
	}
}

func TestDecodePublic_UndecodableRecordIsParked(t *testing.T) {
	t.Parallel()
	rig := newKitchen(t)
	bad := cdc.Record{Topic: kitchenTopic, Partition: 0, Offset: rig.off, Key: json.RawMessage(`{"id":4}`),
		Value: []byte("not json"), Err: cdc.ErrNotChangeEvent}
	rig.off++
	rig.send(bad)
	if rig.parked() != 1 {
		t.Fatal("a record that is not a change event must be parked")
	}
}

// An event of another table never reaches this table.
func TestDecodePublic_EventOfAnotherTableIsParked(t *testing.T) {
	t.Parallel()
	for _, table := range []string{"users", "Kitchen", "kitchens", "public.kitchen", "x"} {
		t.Run(table, func(t *testing.T) {
			rig := newKitchen(t)
			rec := rig.event(cdc.OpCreate, `{"id":1}`, `null`, `{"id":1}`)
			rec.Event.Source.Table = table
			rig.send(rec)
			if rig.parked() != 1 {
				t.Fatalf("an event of table %q was accepted by the kitchen service", table)
			}
			if rig.count() != 0 {
				t.Fatalf("an event of table %q reached the kitchen table", table)
			}
		})
	}
	// An event that does not name its table at all is accepted.
	rig := newKitchen(t)
	rec := rig.event(cdc.OpCreate, `{"id":1}`, `null`, `{"id":1}`)
	rec.Event.Source.Table = ""
	rig.send(rec)
	if rig.parked() != 0 || rig.count() != 1 {
		t.Fatal("an event that does not name its table must be accepted")
	}
}

// Schema evolution: a missing column stays zero, an unknown one is ignored.
func TestDecodePublic_SchemaEvolutionBothWays(t *testing.T) {
	t.Parallel()
	rig := newKitchen(t)
	rig.send(rig.event(cdc.OpCreate, `{"id":7}`, `null`, `{"id":7,"s":"only this"}`))
	if k, ok := rig.row(7); !ok || k.S != "only this" || k.I64 != 0 {
		t.Fatalf("a column the event lacks must stay zero: %+v", k)
	}
	rig.send(rig.event(cdc.OpUpdate, `{"id":7}`, `null`, `{"id":7,"s":"newer","a_column_added_later":"x"}`))
	if k, _ := rig.row(7); k.S != "newer" {
		t.Fatalf("a column the model does not know must be ignored: %+v", k)
	}
	if rig.parked() != 0 {
		t.Fatal("neither case is poison")
	}
}

// Errors never quote the data: the dead letter says why, not what.
func TestDecodePublic_ParkedErrorsDoNotQuoteTheRow(t *testing.T) {
	t.Parallel()
	secret := "TOP-SECRET-12345"
	rows := []string{
		`{"id":1,"i64":"` + secret + `"}`,
		`{"id":1,"t":"` + secret + `"}`,
		`{"id":1,"bytes":"` + secret + `!"}`,
		`{"id":1,"b":"` + secret + `"}`,
		`{"id":1,"d":"` + secret + `"}`,
	}
	rig := newKitchen(t)
	for _, row := range rows {
		rig.send(rig.event(cdc.OpCreate, `{"id":1}`, `null`, row))
	}
	causes := rig.f.deadCauses()
	if len(causes) != len(rows) {
		t.Fatalf("%d parked of %d", len(causes), len(rows))
	}
	for _, c := range causes {
		if strings.Contains(c, secret) {
			t.Fatalf("a dead-letter cause quotes the row: %q", c)
		}
	}
}
