package logger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/schema"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// maxQueryText caps the SQL stored on a span.
const maxQueryText = 8000

// InjectDB traces every bun query (including those inside transactions),
// measures query duration and the connection pool. role names the pool, e.g.
// "writer" or "reader", so the two CQRS databases can be told apart.
func (l *loggerServiceImpl) InjectDB(db *bun.DB, role string) error {
	if err := l.requireStarted(); err != nil {
		return err
	}
	if db == nil {
		return ErrNilTarget
	}
	if role == "" {
		return errors.New("logger: InjectDB needs a role (e.g. writer, reader)")
	}

	attrs := []attribute.KeyValue{semconv.DBSystemPostgreSQL, attribute.String("db.role", role)}

	duration, err := l.state.meter.Float64Histogram("db.client.operation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of database queries."),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return fmt.Errorf("logger: db.client.operation.duration: %w", err)
	}
	// Open/idle/in-use connections and wait time for this pool.
	if _, err := otelsql.RegisterDBStatsMetrics(db.DB, otelsql.WithAttributes(attrs...)); err != nil {
		return fmt.Errorf("logger: db pool metrics: %w", err)
	}

	db.AddQueryHook(&bunQueryHook{tracer: l.state.tracer, duration: duration, attrs: attrs})
	return nil
}

type bunQueryHook struct {
	tracer   trace.Tracer
	duration metric.Float64Histogram
	attrs    []attribute.KeyValue
}

func (h *bunQueryHook) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	ctx, _ = h.tracer.Start(ctx, "db", trace.WithSpanKind(trace.SpanKindClient))
	return ctx
}

func (h *bunQueryHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	op := event.Operation()
	var table string
	if event.IQuery != nil {
		table = event.IQuery.GetTableName()
	}

	labels := append([]attribute.KeyValue{semconv.DBOperationName(op)}, h.attrs...)
	if table != "" {
		labels = append(labels, semconv.DBCollectionName(table))
	}
	failed := event.Err != nil && !errors.Is(event.Err, sql.ErrNoRows) && !errors.Is(event.Err, sql.ErrTxDone)
	metricLabels := labels
	if failed {
		metricLabels = append(metricLabels, semconv.ErrorTypeKey.String(fmt.Sprintf("%T", event.Err)))
	}
	h.duration.Record(ctx, time.Since(event.StartTime).Seconds(), metric.WithAttributes(metricLabels...))

	span := trace.SpanFromContext(ctx)
	defer span.End()
	if !span.IsRecording() {
		return
	}

	name := op
	if table != "" {
		name = op + " " + table
	}
	span.SetName(name)

	fn, file, line := callerOutside("github.com/uptrace/bun")
	span.SetAttributes(labels...)
	span.SetAttributes(
		semconv.DBQueryText(unformattedQuery(event)),
		semconv.CodeFunction(fn),
		semconv.CodeFilepath(file),
		semconv.CodeLineNumber(line),
	)
	if event.Result != nil {
		if n, err := event.Result.RowsAffected(); err == nil {
			span.SetAttributes(attribute.Int64("db.rows_affected", n))
		}
	}
	if errors.Is(event.Err, sql.ErrNoRows) {
		span.AddEvent("no rows")
	}
	if failed {
		span.RecordError(event.Err)
		span.SetStatus(codes.Error, event.Err.Error())
	}
}

// unformattedQuery returns the SQL with ? placeholders instead of values, so
// customer data never ends up in traces.
func unformattedQuery(event *bun.QueryEvent) string {
	q := event.QueryTemplate
	if event.IQuery != nil {
		if b, err := event.IQuery.AppendQuery(schema.NewNopQueryGen(), nil); err == nil {
			q = string(b)
		}
	}
	if len(q) > maxQueryText {
		q = q[:maxQueryText]
	}
	return q
}

// callerOutside reports the first stack frame outside pkg (the code that ran the query).
func callerOutside(pkg string) (fn, file string, line int) {
	var pcs [24]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs[:])])
	for {
		f, more := frames.Next()
		fn, file, line = f.Function, f.File, f.Line
		if !strings.Contains(fn, pkg) && !strings.Contains(fn, "internal/pkg/logger") {
			break
		}
		if !more {
			break
		}
	}
	if i := strings.LastIndexByte(fn, '/'); i >= 0 {
		fn = fn[i+1:]
	}
	return fn, file, line
}

// InjectCache traces and measures a Redis client. Accepts *redis.Client,
// *redis.ClusterClient, *redis.Ring and failover (Sentinel) clients.
func (l *loggerServiceImpl) InjectCache(rdb redis.UniversalClient) error {
	if err := l.requireStarted(); err != nil {
		return err
	}
	if rdb == nil {
		return ErrNilTarget
	}
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		return fmt.Errorf("logger: redis tracing: %w", err)
	}
	if err := redisotel.InstrumentMetrics(rdb); err != nil {
		return fmt.Errorf("logger: redis metrics: %w", err)
	}
	return nil
}
