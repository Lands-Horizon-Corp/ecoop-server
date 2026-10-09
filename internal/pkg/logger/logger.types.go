package logger

import (
	"context"
	"errors"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/pusher/pusher-http-go/v5"
	"github.com/redis/go-redis/v9"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

var (
	// ErrNotStarted is returned by every Inject* call made before Start.
	ErrNotStarted = errors.New("logger: service not started")
	// ErrAlreadyStarted is returned when Start is called twice.
	ErrAlreadyStarted = errors.New("logger: service already started")
	// ErrNilTarget is returned when an Inject* call receives a nil client, handler or config.
	ErrNilTarget = errors.New("logger: nil injection target")
)

// LoggerLevel logs to stdout + Loki and records each call as an event on the
// current span, so every log line is also visible inside its trace in Tempo.
type LoggerLevel interface {
	Span() trace.Span
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	// Success logs at info level and marks the span as OK.
	Success(msg string, kv ...any)
	// Error logs, records err on the span and marks the span as failed.
	Error(err error, msg string, kv ...any)
	// Fatal is Error plus an immediate flush of all telemetry, so the record
	// survives a crash. It does not exit the process; the caller decides that.
	Fatal(err error, msg string, kv ...any)
	// Event adds a named span event without writing a log line above debug level.
	Event(name string, kv ...any)
}

type LogContextService interface {
	context.Context
	Trace(name string, attrs ...attribute.KeyValue) (LogContextService, LoggerLevel)
}

// JobFunc is the body of a background job run through LoggerService.Job.
type JobFunc func(ctx LogContextService, log LoggerLevel) error

type LoggerService interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	Meter() metric.Meter
	NewContext(parent context.Context) LogContextService

	// Job runs fn as a root span (cron, worker, startup task), records
	// job.duration / job.runs metrics, and converts panics into errors.
	Job(ctx context.Context, name string, fn JobFunc, attrs ...attribute.KeyValue) error

	// Datastores
	// InjectDB instruments a bun database; call it once per pool (writer, reader).
	InjectDB(db *bun.DB, role string) error
	InjectCache(rdb redis.UniversalClient) error

	// HTTP
	InjectHertz(h *server.Hertz) error
	InjectHertzClient(c *client.Client) error
	InjectHTTPClient(c *http.Client) error
	InjectPusher(c *pusher.Client) error

	// gRPC instrumentation can only be set at construction:
	// grpc.NewServer(log.GRPCServerOptions()...), grpc.NewClient(addr, log.GRPCClientOptions()...).
	GRPCServerOptions() []grpc.ServerOption
	GRPCClientOptions() []grpc.DialOption

	// Kafka
	InjectBrokerConfig(cfg *sarama.Config) error
	InjectBrokerSyncProducer(cfg *sarama.Config, p sarama.SyncProducer) (sarama.SyncProducer, error)
	InjectBrokerAsyncProducer(cfg *sarama.Config, p sarama.AsyncProducer) (sarama.AsyncProducer, error)
	InjectBrokerConsumerGroup(h sarama.ConsumerGroupHandler) (sarama.ConsumerGroupHandler, error)
	InjectBrokerConsumer(c sarama.Consumer) (sarama.Consumer, error)
	InjectBrokerPartitionConsumer(pc sarama.PartitionConsumer) (sarama.PartitionConsumer, error)
	// BrokerMessageWithContext writes the trace from ctx into the message headers
	// so the consumer continues the same trace.
	BrokerMessageWithContext(ctx context.Context, msg *sarama.ProducerMessage)
	// BrokerMessageContext reads the trace from a consumed message's headers.
	BrokerMessageContext(ctx context.Context, msg *sarama.ConsumerMessage) context.Context
}
