package observability_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

func assertSafeLabel(t *testing.T, what, got string) {
	t.Helper()
	if len(got) > obs.MaxLabelValueLen {
		t.Errorf("%s is %d bytes, limit %d", what, len(got), obs.MaxLabelValueLen)
	}
	if !utf8.ValidString(got) {
		t.Errorf("%s is not valid UTF-8: %q", what, got)
	}
	for _, r := range got {
		if unicode.IsControl(r) {
			t.Errorf("%s holds a control character: %q", what, got)
			break
		}
	}
}

// A hostile label value is made safe and the operation is still counted.
func TestPoison_HostileLabelValues(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"1 MiB":               strings.Repeat("x", 1<<20),
		"NUL":                 "ac\x00count",
		"newlines":            "a\nb\r\nc",
		"invalid UTF-8":       "ab\xff\xfe\xc3",
		"separators":          `a,outcome=error|"q"\`,
		"multibyte at limit":  strings.Repeat("é", 500),
		"emoji at the limit":  strings.Repeat("a", obs.MaxLabelValueLen-1) + "😀",
		"only control":        "\x01\x02\x03",
		"empty":               "",
		"label-looking value": "table=accounts,outcome=success",
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := newService(t)
			_, op := s.Begin(context.Background(), obs.ComponentPagination, "count", slog.String(obs.KeyTable, v))
			op.End(nil)
			sm := find(t, s, obs.MetricOperations, map[string]string{obs.KeyOutcome: "success"})
			if sm.Value != 1 {
				t.Fatalf("counted %v, want 1", sm.Value)
			}
			assertSafeLabel(t, "table", sm.Attrs[obs.KeyTable])
		})
	}
}

// Operation and component names are labels too, and get the same treatment.
func TestPoison_HostileOperationAndComponentNames(t *testing.T) {
	t.Parallel()
	s := newService(t)
	_, op := s.Begin(context.Background(), obs.Component(strings.Repeat("c\n", 1<<19)), strings.Repeat("o\x00", 1<<19))
	op.End(nil)
	for _, sm := range s.Snapshot() {
		assertSafeLabel(t, sm.Name+" component", sm.Attrs[obs.KeyComponent])
		assertSafeLabel(t, sm.Name+" operation", sm.Attrs[obs.KeyOperation])
	}
}

// A flood of attributes with made-up keys never creates a series.
func TestPoison_AttributeFloodCreatesNoSeries(t *testing.T) {
	t.Parallel()
	s := newService(t)
	attrs := make([]slog.Attr, 10_000)
	for i := range attrs {
		attrs[i] = slog.String(fmt.Sprintf("k-%d", i), fmt.Sprintf("v-%d", i))
	}
	_, op := s.Begin(context.Background(), obs.ComponentSQL, "ping", attrs...)
	op.SetAttributes(attrs...)
	op.End(nil)
	if n := len(s.Snapshot()); n > 4 {
		t.Fatalf("%d series from one operation, want at most 4", n)
	}
}

// A value that looks like another label must not merge two different series into one.
func TestPoison_LookalikeValueDoesNotCollide(t *testing.T) {
	t.Parallel()
	s := newService(t)
	_, a := s.Begin(context.Background(), obs.ComponentPagination, "count", slog.String(obs.KeyTable, "x,topic=y"))
	a.End(nil)
	_, b := s.Begin(context.Background(), obs.ComponentPagination, "count", slog.String(obs.KeyTable, "x"), slog.String(obs.KeyTopic, "y"))
	b.End(nil)
	n := 0
	for _, sm := range s.Snapshot() {
		if sm.Name == obs.MetricOperations {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("%d operation series, want 2: two different label sets were merged", n)
	}
}

// Whatever the value, an identifier must never come out as a bare, unbounded label.
func TestPoison_UserIdentifiersAreBoundedAndCapped(t *testing.T) {
	t.Parallel()
	s := newService(t)
	n := obs.MaxSeries + 2000
	for i := range n {
		_, op := s.Begin(context.Background(), obs.ComponentPagination, "count",
			slog.String(obs.KeyTable, fmt.Sprintf("11111111-2222-3333-4444-%012d", i)))
		op.End(nil)
	}
	if got := len(s.Snapshot()); got > obs.MaxSeries+16 {
		t.Fatalf("%d series after %d distinct identifiers, want at most %d", got, n, obs.MaxSeries+16)
	}
	if s.Overflowed() == 0 {
		t.Fatal("Overflowed() is 0 although the cap was exceeded")
	}
	var total float64
	for _, sm := range s.Snapshot() {
		if sm.Name == obs.MetricOperations {
			total += sm.Value
		}
	}
	if total != float64(n) {
		t.Fatalf("counted %v operations in total, want %d: the overflow lost counts", total, n)
	}
	if _, ok := s.Find(obs.MetricOperations, map[string]string{obs.KeyOverflow: "true"}); !ok {
		t.Fatal("no overflow series, so the drop is invisible to an operator")
	}
}
