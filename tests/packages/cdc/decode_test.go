//go:build integration

package cdc_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// Decode and Topic are pure: no Kafka needed. They are the first thing every record
// passes through, so each shape Debezium (or a hostile producer) can send is pinned.

func TestDecode_EveryOperation(t *testing.T) {
	t.Parallel()
	for _, op := range []cdc.ChangeType{cdc.OpCreate, cdc.OpUpdate, cdc.OpDelete, cdc.OpRead, cdc.OpTruncate} {
		t.Run(string(op), func(t *testing.T) {
			ev, err := cdc.Decode(fmt.Appendf(nil, `{"op":%q,"before":null,"after":{"id":1},"ts_ms":5}`, op))
			if err != nil || ev == nil || ev.Op != op || ev.TsMs != 5 {
				t.Fatalf("Decode = %+v, %v", ev, err)
			}
		})
	}
}

func TestDecode_EmptyValueIsATombstone(t *testing.T) {
	t.Parallel()
	for name, v := range map[string][]byte{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			ev, err := cdc.Decode(v)
			if ev != nil || err != nil {
				t.Fatalf("a tombstone has neither event nor error: %+v, %v", ev, err)
			}
		})
	}
}

func TestDecode_SourceFields(t *testing.T) {
	t.Parallel()
	ev, err := cdc.Decode([]byte(`{"op":"u","before":null,"after":{"id":3},
		"source":{"db":"ecoop","schema":"public","table":"accounts","lsn":123456789012,"txId":77},"ts_ms":9}`))
	if err != nil {
		t.Fatal(err)
	}
	s := ev.Source
	if s.DB != "ecoop" || s.Schema != "public" || s.Table != "accounts" || s.LSN != 123456789012 || s.TxID != 77 {
		t.Fatalf("source = %+v", s)
	}
}

func TestDecode_LSNBeyondInt32IsKept(t *testing.T) {
	t.Parallel()
	ev, err := cdc.Decode([]byte(`{"op":"c","after":{"id":1},"source":{"lsn":9223372036854775807}}`))
	if err != nil || ev.Source.LSN != 9223372036854775807 {
		t.Fatalf("LSN = %d, %v", ev.Source.LSN, err)
	}
}

func TestDecode_SchemaEnvelopeIsUnwrapped(t *testing.T) {
	t.Parallel()
	wrapped := `{"schema":{"type":"struct"},"payload":{"op":"c","before":null,"after":{"id":4},"ts_ms":1}}`
	ev, err := cdc.Decode([]byte(wrapped))
	if err != nil || ev.Op != cdc.OpCreate || string(ev.After) != `{"id":4}` {
		t.Fatalf("Decode = %+v, %v", ev, err)
	}
}

func TestDecode_BeforeAndAfterAreKeptRaw(t *testing.T) {
	t.Parallel()
	ev, err := cdc.Decode([]byte(`{"op":"u","before":{"id":1,"n":"old"},"after":{"id":1,"n":"new"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ev.Before), `"old"`) || !strings.Contains(string(ev.After), `"new"`) {
		t.Fatalf("before=%s after=%s", ev.Before, ev.After)
	}
}

func TestDecode_Refusals(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"not json":                 `nope`,
		"truncated json":           `{"op":"c","after":`,
		"json array":               `[1,2]`,
		"json string":              `"hello"`,
		"json number":              `42`,
		"json null literal":        `null`,
		"empty object":             `{}`,
		"object without op":        `{"after":{"id":1}}`,
		"empty op":                 `{"op":""}`,
		"payload without op":       `{"payload":{"x":1}}`,
		"schema without payload":   `{"schema":{"type":"struct"}}`,
		"op of the wrong type":     `{"op":5}`,
		"leading garbage":          `x{"op":"c"}`,
		"html":                     `<html></html>`,
		"binary":                   "\x00\x01\x02\xff",
		"whitespace only":          "   \n\t ",
		"nested payload not an op": `{"payload":{"payload":{"op":"c"}}}`,
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			ev, err := cdc.Decode([]byte(v))
			if !errors.Is(err, cdc.ErrNotChangeEvent) || ev != nil {
				t.Fatalf("Decode(%q) = %+v, %v; want ErrNotChangeEvent", v, ev, err)
			}
		})
	}
}

func TestDecode_UnknownOperationIsStillADecodedEvent(t *testing.T) {
	t.Parallel()
	// An operation this code does not know is not malformed: the consumer decides
	// (the CQRS service counts it as "unsupported operation" and moves on).
	ev, err := cdc.Decode([]byte(`{"op":"m","after":null}`))
	if err != nil || ev == nil || ev.Op != "m" {
		t.Fatalf("Decode = %+v, %v", ev, err)
	}
}

func TestDecode_VeryLargeValue(t *testing.T) {
	t.Parallel()
	big := strings.Repeat("x", 4<<20)
	ev, err := cdc.Decode(fmt.Appendf(nil, `{"op":"c","after":{"id":1,"blob":%q}}`, big))
	if err != nil || ev == nil || len(ev.After) < len(big) {
		t.Fatalf("a 4 MiB value must decode: err=%v", err)
	}
}

func TestDecode_UnicodeSurvives(t *testing.T) {
	t.Parallel()
	ev, err := cdc.Decode([]byte(`{"op":"c","after":{"id":1,"name":"Zoë 日本語 🎉"}}`))
	if err != nil || !strings.Contains(string(ev.After), "日本語 🎉") {
		t.Fatalf("After = %s, %v", ev.After, err)
	}
}

func TestDecode_ConcurrentCallsAgree(t *testing.T) {
	t.Parallel()
	done := make(chan error, 32)
	for i := range 32 {
		go func() {
			ev, err := cdc.Decode(fmt.Appendf(nil, `{"op":"c","after":{"id":%d},"source":{"lsn":%d}}`, i, i*10))
			switch {
			case err != nil:
				done <- err
			case ev.Source.LSN != int64(i*10):
				done <- fmt.Errorf("goroutine %d got LSN %d", i, ev.Source.LSN)
			default:
				done <- nil
			}
		}()
	}
	for range 32 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestTopic_Naming(t *testing.T) {
	t.Parallel()
	cases := []struct{ prefix, schema, table, want string }{
		{"ecoop", "public", "users", "ecoop.public.users"},
		{"acme", "billing", "invoices", "acme.billing.invoices"},
		{"", "public", "users", "ecoop.public.users"},
		{"   ", "public", "users", "ecoop.public.users"},
		{"a.b", "public", "t", "a.b.public.t"},
		{"ecoop", "", "t", "ecoop..t"},
		{"ecoop", "public", "Mixed_Case", "ecoop.public.Mixed_Case"},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%q/%s.%s", c.prefix, c.schema, c.table), func(t *testing.T) {
			if got := cdc.NewCDCService(nil, "", c.prefix).Topic(c.schema, c.table); got != c.want {
				t.Fatalf("Topic = %q, want %q", got, c.want)
			}
		})
	}
}

func TestTopic_NilServiceIsEmpty(t *testing.T) {
	t.Parallel()
	var s *cdc.CDCService
	if got := s.Topic("public", "t"); got != "" {
		t.Fatalf("Topic on a nil service = %q", got)
	}
}

func TestGroupID_TrimmedAndNilSafe(t *testing.T) {
	t.Parallel()
	if got := cdc.NewCDCService(nil, "  grp  ", "").GroupID(); got != "grp" {
		t.Fatalf("GroupID = %q", got)
	}
	var s *cdc.CDCService
	if got := s.GroupID(); got != "" {
		t.Fatalf("GroupID on a nil service = %q", got)
	}
}

func TestConstants_AreSane(t *testing.T) {
	t.Parallel()
	if cdc.DefaultMaxBatch <= 0 || cdc.DefaultMaxDeadLetters <= 0 {
		t.Fatalf("defaults must be positive: %d %d", cdc.DefaultMaxBatch, cdc.DefaultMaxDeadLetters)
	}
	ops := map[cdc.ChangeType]string{cdc.OpCreate: "c", cdc.OpUpdate: "u", cdc.OpDelete: "d", cdc.OpRead: "r", cdc.OpTruncate: "t"}
	for op, want := range ops {
		if string(op) != want {
			t.Errorf("operation %q must be Debezium's %q", op, want)
		}
	}
	headers := []string{cdc.HeaderDLQTopic, cdc.HeaderDLQPartition, cdc.HeaderDLQOffset, cdc.HeaderDLQError,
		cdc.HeaderDLQAttempts, cdc.HeaderDLQGroup, cdc.HeaderDLQFailedAt}
	seen := map[string]bool{}
	for _, h := range headers {
		if h == "" || seen[h] || !strings.HasPrefix(h, "dlq.") {
			t.Errorf("header %q must be unique, non-empty and start with dlq.", h)
		}
		seen[h] = true
	}
}

func TestErrors_AreDistinct(t *testing.T) {
	t.Parallel()
	all := []error{cdc.ErrNoService, cdc.ErrNoBrokers, cdc.ErrNoGroup, cdc.ErrNoTopic, cdc.ErrNoHandler,
		cdc.ErrDuplicateSubscription, cdc.ErrConnect, cdc.ErrNotChangeEvent, cdc.ErrNoBatchHandler,
		cdc.ErrNilContext, cdc.ErrNotRunning, cdc.ErrTooManyDeadLetters}
	for i, a := range all {
		if a == nil || a.Error() == "" || !strings.HasPrefix(a.Error(), "cdc:") {
			t.Errorf("error %d must be non-nil with a cdc: prefix: %v", i, a)
		}
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Errorf("errors %d and %d must be distinct", i, j)
			}
		}
	}
}
