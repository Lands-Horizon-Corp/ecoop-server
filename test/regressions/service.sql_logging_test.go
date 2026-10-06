package regressions

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// logEvent is one line reported through an injected logger: the span it was written in, its level,
// the message, the error and the structured fields.
type logEvent struct {
	span, level, msg string
	err              error
	fields           map[string]any
}

// recordingLog is a LogContextService that records instead of exporting, so no collector is needed.
type recordingLog struct {
	context.Context
	mu     sync.Mutex
	events []logEvent
}

func (r *recordingLog) Trace(name string, _ ...attribute.KeyValue) (logger.LogContextService, logger.LoggerLevel) {
	return r, &recordingLevel{r: r, span: name}
}

func (r *recordingLog) With(attribute.KeyValue) logger.LogContextService { return r }

func (r *recordingLog) Observe(name string, fn func() error, _ ...attribute.KeyValue) error {
	lvl := &recordingLevel{r: r, span: name}
	if err := fn(); err != nil {
		lvl.Error(err, name+" failed")
		return err
	}
	lvl.Info(name + " done")
	return nil
}
func (r *recordingLog) Emit(name string, write func(logger.LoggerLevel)) {
	write(&recordingLevel{r: r, span: name})
}

func (r *recordingLog) Start(context.Context) error { return nil }
func (r *recordingLog) Stop(context.Context) error  { return nil }

type recordingLevel struct {
	r    *recordingLog
	span string
}

func (l *recordingLevel) add(level, msg string, err error, kv []any) {
	fields := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		if k, ok := kv[i].(string); ok {
			fields[k] = kv[i+1]
		}
	}
	l.r.mu.Lock()
	defer l.r.mu.Unlock()
	l.r.events = append(l.r.events, logEvent{span: l.span, level: level, msg: msg, err: err, fields: fields})
}

func (l *recordingLevel) Span() trace.Span            { return trace.SpanFromContext(context.Background()) }
func (l *recordingLevel) Debug(msg string, kv ...any) { l.add("debug", msg, nil, kv) }
func (l *recordingLevel) Info(msg string, kv ...any)  { l.add("info", msg, nil, kv) }
func (l *recordingLevel) Warn(msg string, kv ...any)  { l.add("warn", msg, nil, kv) }
func (l *recordingLevel) Error(err error, msg string, kv ...any) {
	l.add("error", msg, err, kv)
}
func (l *recordingLevel) Fatal(err error, msg string, kv ...any) {
	l.add("fatal", msg, err, kv)
}

// await waits for a line matching match and returns it; logs from background goroutines arrive late.
func (r *recordingLog) await(t *testing.T, what string, match func(logEvent) bool) logEvent {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		r.mu.Lock()
		for _, e := range r.events {
			if match(e) {
				r.mu.Unlock()
				return e
			}
		}
		r.mu.Unlock()
		select {
		case <-deadline:
			t.Fatalf("no log line for %s; got %+v", what, r.snapshot())
			return logEvent{}
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (r *recordingLog) snapshot() []logEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]logEvent(nil), r.events...)
}

func withMsg(msg string) func(logEvent) bool { return func(e logEvent) bool { return e.msg == msg } }

// Every Run, Stop and migration call reaches the injected logger, failures included, and the service
// still behaves exactly as it does without one.
func TestSQLService_ReportsEveryOperationToTheInjectedLogger(t *testing.T) {
	e := newSQLEnv(t)
	e.write(1, "users", `CREATE TABLE users (id int);`, `DROP TABLE users;`)
	dir, err := os.Open(e.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dir.Close() })

	log := &recordingLog{Context: bg}
	svc := sqlsvc.NewSQLService(e.dsn, 2, 5, dir, false, e.status, nil, log)

	if err := svc.Run(bg); err != nil {
		t.Fatal(err)
	}
	if err := svc.Ping(bg); err != nil { // not instrumented: the query hot path stays quiet
		t.Fatal(err)
	}
	if err := svc.Migrate(bg); err != nil {
		t.Fatal(err)
	}
	if !e.hasTable("users") {
		t.Fatal("the wrapped service did not migrate")
	}
	stepsErr := svc.RollbackSteps(bg, 0)
	if !errors.Is(stepsErr, sqlsvc.ErrInvalidSteps) {
		t.Fatalf("RollbackSteps(0) = %v; want ErrInvalidSteps", stepsErr)
	}
	_, diffErr := svc.Diff(bg, "x")
	if !errors.Is(diffErr, sqlsvc.ErrNoModels) {
		t.Fatalf("Diff without models = %v; want ErrNoModels", diffErr)
	}
	if err := svc.Stop(bg); err != nil {
		t.Fatal(err)
	}

	want := []logEvent{
		{span: "sql.run", level: "info"},
		{span: "sql.migrate", level: "info"},
		{span: "sql.rollback_steps", level: "error", err: stepsErr},
		{span: "sql.diff", level: "error", err: diffErr},
		{span: "sql.stop", level: "info"},
	}
	if len(log.events) != len(want) {
		t.Fatalf("logged %d events, want %d: %+v", len(log.events), len(want), log.events)
	}
	for i, w := range want {
		if got := log.events[i]; got.span != w.span || got.level != w.level || !errors.Is(got.err, w.err) {
			t.Errorf("event %d = %+v; want %+v", i, got, w)
		}
	}
}
