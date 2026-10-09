package logger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/XSAM/otelsql"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/pusher/pusher-http-go/v5"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/dnwe/otelsarama"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type state struct {
	name     string
	tp       *sdktrace.TracerProvider
	lp       *sdklog.LoggerProvider
	tracer   trace.Tracer
	z        *zap.Logger
	buffered *zapcore.BufferedWriteSyncer
}

type loggerServiceImpl struct {
	state     state
	logLevel  string
	logFormat string
	Context   context.Context
}

func NewLoggerService(serviceName, logLevel, logFormat string) LoggerService {
	return &loggerServiceImpl{
		state:     state{name: serviceName},
		logLevel:  logLevel,
		logFormat: logFormat,
	}
}

func (l *loggerServiceImpl) Start(ctx context.Context) error {
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", l.state.name)),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil {
		return err
	}

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		fmt.Fprintln(os.Stderr, "otel:", err)
	}))

	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return fmt.Errorf("otlp trace exporter: %w", err)
	}
	l.state.tp = sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExp),
	)
	otel.SetTracerProvider(l.state.tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	logExp, err := otlploggrpc.New(ctx)
	if err != nil {
		return fmt.Errorf("otlp log exporter: %w", err)
	}
	l.state.lp = sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
	)
	global.SetLoggerProvider(l.state.lp)

	l.state.tracer = otel.Tracer(l.state.name)

	level := zapcore.InfoLevel
	_ = level.UnmarshalText([]byte(l.logLevel))

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "timestamp"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	var enc zapcore.Encoder
	if l.logFormat == "json" {
		enc = zapcore.NewJSONEncoder(encCfg)
	} else {
		encCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
		enc = zapcore.NewConsoleEncoder(encCfg)
	}

	buffered := &zapcore.BufferedWriteSyncer{
		WS:            zapcore.AddSync(os.Stderr),
		FlushInterval: time.Second,
		Size:          256 * 1024,
	}

	cores := []zapcore.Core{zapcore.NewCore(enc, buffered, level)}
	if l.state.lp != nil {
		cores = append(cores, otelzap.NewCore(l.state.name, otelzap.WithLoggerProvider(l.state.lp)))
	}
	l.state.z = zap.New(
		zapcore.NewTee(cores...),
		zap.AddCaller(),
		zap.AddCallerSkip(2),
		zap.WithFatalHook(zapcore.WriteThenNoop),
	)
	l.state.buffered = buffered
	l.Context = ctx
	return nil
}

func (l *loggerServiceImpl) Stop(ctx context.Context) error {
	if l.state.buffered != nil {
		_ = l.state.buffered.Sync()
	}
	if l.state.tp != nil {
		_ = l.state.tp.Shutdown(ctx)
	}
	if l.state.lp != nil {
		_ = l.state.lp.Shutdown(ctx)
	}
	return nil
}

func (l *loggerServiceImpl) NewContext(parent context.Context) LogContextService {
	if parent == nil {
		parent = context.Background()
	}
	return &logContextImpl{
		Context: parent,
		tracer:  l.state.tracer,
		z:       l.state.z,
	}
}

// InjectHertz must be called before routes are registered, since Hertz only
// applies middleware to routes added after Use.
func (l *loggerServiceImpl) InjectHertz(h *server.Hertz) {
	// Hertz's built-in tracer hook can only be set at construction, so spans are created here instead.
	propagator := otel.GetTextMapPropagator()
	h.Use(func(ctx context.Context, c *app.RequestContext) {
		ctx = propagator.Extract(ctx, hertzHeaderCarrier{&c.Request.Header})

		method := string(c.Method())
		name := method
		if route := c.FullPath(); route != "" {
			name = method + " " + route
		}
		ctx, span := l.state.tracer.Start(ctx, name,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(method),
				semconv.URLPath(string(c.Path())),
				semconv.HTTPRoute(c.FullPath()),
			),
		)
		defer span.End()

		c.Next(ctx)

		status := c.Response.StatusCode()
		span.SetAttributes(semconv.HTTPResponseStatusCode(status))
		if err := c.Errors.Last(); err != nil {
			span.RecordError(err)
		}
		if status >= 500 {
			span.SetStatus(codes.Error, "")
		}
	})
}

// InjectDB wraps the driver's connector so every query is traced. An already
// opened *sql.DB cannot be instrumented, so pass the connector instead
// (e.g. stdlib.GetConnector(cfg) for pgx, pq.NewConnector(dsn) for lib/pq).
func (l *loggerServiceImpl) InjectDB(connector driver.Connector) *sql.DB {
	attrs := otelsql.WithAttributes(semconv.DBSystemPostgreSQL)
	db := otelsql.OpenDB(connector, attrs)
	// Register open/idle connection stats gauges
	_, _ = otelsql.RegisterDBStatsMetrics(db, attrs)
	return db
}

func (l *loggerServiceImpl) InjectCache(rdb *redis.Client) *redis.Client {
	_ = redisotel.InstrumentTracing(rdb)
	_ = redisotel.InstrumentMetrics(rdb)
	return rdb
}

func (l *loggerServiceImpl) InjectBrokerProducer(producer sarama.SyncProducer) sarama.SyncProducer {
	return otelsarama.WrapSyncProducer(nil, producer)
}

func (l *loggerServiceImpl) InjectBrokerConsumer(handler sarama.ConsumerGroupHandler) sarama.ConsumerGroupHandler {
	return otelsarama.WrapConsumerGroupHandler(handler)
}

func (l *loggerServiceImpl) InjectPusher(client *pusher.Client) *pusher.Client {
	if client.HTTPClient == nil {
		// Same default the pusher library would otherwise create lazily.
		client.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	client.HTTPClient.Transport = otelhttp.NewTransport(client.HTTPClient.Transport)
	return client
}

type hertzHeaderCarrier struct {
	h *protocol.RequestHeader
}

func (c hertzHeaderCarrier) Get(key string) string { return string(c.h.Peek(key)) }

func (c hertzHeaderCarrier) Set(key, value string) { c.h.Set(key, value) }

func (c hertzHeaderCarrier) Keys() []string {
	var keys []string
	c.h.VisitAll(func(k, _ []byte) { keys = append(keys, string(k)) })
	return keys
}
