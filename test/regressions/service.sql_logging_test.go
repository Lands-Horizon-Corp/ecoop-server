package regressions

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// logEvent is what the SQL service reported through its injected logger.
type logEvent struct {
	span, level string
	err         error
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

func (r *recordingLog) Observe(name string, fn func() error, _ ...attribute.KeyValue) error {
	lvl := &recordingLevel{r: r, span: name}
	if err := fn(); err != nil {
		lvl.Error(err, name+" failed")
		return err
	}
	lvl.Info(name + " done")
	return nil
}
func (r *recordingLog) Start(context.Context) error { return nil }
func (r *recordingLog) Stop(context.Context) error  { return nil }

type recordingLevel struct {
	r    *recordingLog
	span string
}

func (l *recordingLevel) add(level string, err error) {
	l.r.mu.Lock()
	defer l.r.mu.Unlock()
	l.r.events = append(l.r.events, logEvent{span: l.span, level: level, err: err})
}

func (l *recordingLevel) Span() trace.Span                    { return trace.SpanFromContext(context.Background()) }
func (l *recordingLevel) Debug(string, ...any)                { l.add("debug", nil) }
func (l *recordingLevel) Info(string, ...any)                 { l.add("info", nil) }
func (l *recordingLevel) Warn(string, ...any)                 { l.add("warn", nil) }
func (l *recordingLevel) Error(err error, _ string, _ ...any) { l.add("error", err) }
func (l *recordingLevel) Fatal(err error, _ string, _ ...any) { l.add("fatal", err) }

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
