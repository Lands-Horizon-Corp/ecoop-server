package logger

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/IBM/sarama"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/pusher/pusher-http-go/v5"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type LoggerLevel interface {
	Span() trace.Span
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(err error, msg string, kv ...any)
	Fatal(err error, msg string, kv ...any)
}

type LogContextService interface {
	context.Context
	Trace(name string, attrs ...attribute.KeyValue) (LogContextService, LoggerLevel)
}

type LoggerService interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	InjectDB(connector driver.Connector) *sql.DB
	InjectCache(rdb *redis.Client) *redis.Client
	InjectBrokerProducer(producer sarama.SyncProducer) sarama.SyncProducer
	InjectBrokerConsumer(handler sarama.ConsumerGroupHandler) sarama.ConsumerGroupHandler
	InjectPusher(client *pusher.Client) *pusher.Client
	InjectHertz(h *server.Hertz)

	NewContext(parent context.Context) LogContextService
}
