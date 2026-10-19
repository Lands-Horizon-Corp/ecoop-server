package obstest_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

// The helpers every observer test relies on are themselves tested: a helper
// that always said zero would make every "was not counted" test pass for the
// wrong reason.

func TestCount_ReadsWhatWasRecorded(t *testing.T) {
	t.Parallel()
	o := obstest.New(t)
	for range 3 {
		_, op := o.Begin(context.Background(), observability.ComponentCache, "get")
		op.End(nil)
	}
	_, op := o.Begin(context.Background(), observability.ComponentCache, "get")
	op.End(errors.New("x"))
	_, op = o.Begin(context.Background(), observability.ComponentCache, "set")
	op.End(nil)
	_, op = o.Begin(context.Background(), observability.ComponentSQL, "get")
	op.End(nil)

	cases := []struct {
		component, op, outcome string
		want                   float64
	}{
		{"cache", "get", "success", 3},
		{"cache", "get", "error", 1},
		{"cache", "set", "success", 1},
		{"cache", "set", "error", 0},
		{"sql", "get", "success", 1},
		{"cache", "nope", "success", 0},
		{"nope", "get", "success", 0},
		{"cache", "", "success", 4}, // every cache operation
		{"cache", "", "error", 1},
		{"sql", "", "success", 1},
	}
	for _, c := range cases {
		if got := obstest.Count(o, c.component, c.op, c.outcome); got != c.want {
			t.Errorf("Count(%q, %q, %q) = %v, want %v", c.component, c.op, c.outcome, got, c.want)
		}
	}
}

func TestCounter_BindsAComponent(t *testing.T) {
	t.Parallel()
	o := obstest.New(t)
	_, op := o.Begin(context.Background(), observability.ComponentQR, "encode")
	op.End(nil)
	qrCount := obstest.Counter("qr")
	if got := qrCount(o, "encode", "success"); got != 1 {
		t.Fatalf("bound counter = %v, want 1", got)
	}
	if got := obstest.Counter("sql")(o, "encode", "success"); got != 0 {
		t.Fatalf("another component's counter = %v, want 0", got)
	}
}

// failRecorder lets a test observe that a helper failed, without failing itself.
type failRecorder struct {
	testing.TB
	failed bool
	msg    string
}

func (f *failRecorder) Helper() {}
func (f *failRecorder) Fatalf(format string, args ...any) {
	f.failed = true
	f.msg = strings.TrimSpace(format)
	panic(f) // stop the helper the way Fatalf stops a test
}
func (f *failRecorder) Errorf(format string, args ...any) { f.failed = true; f.msg = format }

func fails(fn func(tb testing.TB)) (failed bool) {
	rec := &failRecorder{}
	defer func() {
		if r := recover(); r != nil && r != any(rec) {
			panic(r)
		}
		failed = rec.failed
	}()
	fn(rec)
	return
}

func TestNoLeak_FindsALeakAndPassesACleanRegistry(t *testing.T) {
	t.Parallel()
	o := obstest.New(t)
	_, op := o.Begin(context.Background(), observability.ComponentSQL, "ping", slog.String(observability.KeyTable, "accounts"))
	op.End(nil)

	if fails(func(tb testing.TB) { obstest.NoLeak(tb, o, "secret-token", "") }) {
		t.Error("NoLeak failed on a clean registry")
	}
	if !fails(func(tb testing.TB) { obstest.NoLeak(tb, o, "accounts") }) {
		t.Error("NoLeak missed a value that is in a label")
	}
	if !fails(func(tb testing.TB) { obstest.NoLeak(tb, o, "operations_total") }) {
		t.Error("NoLeak missed a value that is in a series name")
	}
}

func TestNoSeries_FailsWhenSomethingWasCounted(t *testing.T) {
	t.Parallel()
	o := obstest.New(t)
	if fails(func(tb testing.TB) { obstest.NoSeries(tb, o, "sql", "ping", "success", "error") }) {
		t.Error("NoSeries failed on an empty registry")
	}
	_, op := o.Begin(context.Background(), observability.ComponentSQL, "ping")
	op.End(nil)
	if !fails(func(tb testing.TB) { obstest.NoSeries(tb, o, "sql", "ping", "success") }) {
		t.Error("NoSeries missed a counted operation")
	}
}

func TestNothingInFlight(t *testing.T) {
	t.Parallel()
	o := obstest.New(t)
	_, op := o.Begin(context.Background(), observability.ComponentSQL, "ping")
	if !fails(func(tb testing.TB) { obstest.NothingInFlight(tb, o) }) {
		t.Error("NothingInFlight missed a running operation")
	}
	op.End(nil)
	if fails(func(tb testing.TB) { obstest.NothingInFlight(tb, o) }) {
		t.Error("NothingInFlight failed after the operation ended")
	}
}
