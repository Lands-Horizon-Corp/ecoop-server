package logger

import (
	"context"
	"fmt"
	"strings"

	"github.com/IBM/sarama"
	"github.com/dnwe/otelsarama"
	"github.com/rcrowley/go-metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// InjectBrokerConfig exports sarama's client metrics (request/response rates,
// latency, bytes in/out, batch sizes, per broker and per topic) and must be
// called on the same config used to build the producer, consumer or client.
func (l *loggerServiceImpl) InjectBrokerConfig(cfg *sarama.Config) error {
	if err := l.requireStarted(); err != nil {
		return err
	}
	if cfg == nil {
		return ErrNilTarget
	}
	if metrics.UseNilMetrics {
		return fmt.Errorf("logger: kafka metrics disabled (metrics.UseNilMetrics is true)")
	}
	if cfg.MetricRegistry == nil {
		cfg.MetricRegistry = metrics.NewRegistry()
	}
	reg := cfg.MetricRegistry
	clientID := attribute.String("messaging.client.id", cfg.ClientID)

	rate, err := l.state.meter.Float64ObservableGauge("messaging.kafka.client.rate",
		metric.WithUnit("{event}/s"),
		metric.WithDescription("Sarama meters: one-minute rate, by metric name (e.g. request-rate, record-send-rate)."),
	)
	if err != nil {
		return fmt.Errorf("logger: messaging.kafka.client.rate: %w", err)
	}
	total, err := l.state.meter.Int64ObservableGauge("messaging.kafka.client.total",
		metric.WithDescription("Sarama meters and counters: running total, by metric name."),
	)
	if err != nil {
		return fmt.Errorf("logger: messaging.kafka.client.total: %w", err)
	}
	dist, err := l.state.meter.Float64ObservableGauge("messaging.kafka.client.distribution",
		metric.WithDescription("Sarama histograms (e.g. request-latency-in-ms, batch-size) by metric name and stat (mean, p95, p99, max)."),
	)
	if err != nil {
		return fmt.Errorf("logger: messaging.kafka.client.distribution: %w", err)
	}
	value, err := l.state.meter.Float64ObservableGauge("messaging.kafka.client.value",
		metric.WithDescription("Sarama gauges (e.g. requests-in-flight), by metric name."),
	)
	if err != nil {
		return fmt.Errorf("logger: messaging.kafka.client.value: %w", err)
	}

	_, err = l.state.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		reg.Each(func(name string, m any) {
			attrs := append(saramaMetricAttrs(name), clientID)
			switch m := m.(type) {
			case metrics.Meter:
				s := m.Snapshot()
				o.ObserveFloat64(rate, s.Rate1(), metric.WithAttributes(attrs...))
				o.ObserveInt64(total, s.Count(), metric.WithAttributes(attrs...))
			case metrics.Histogram:
				s := m.Snapshot()
				for _, st := range []struct {
					name string
					v    float64
				}{
					{"mean", s.Mean()}, {"p95", s.Percentile(0.95)}, {"p99", s.Percentile(0.99)}, {"max", float64(s.Max())},
				} {
					o.ObserveFloat64(dist, st.v, metric.WithAttributes(append(attrs, attribute.String("stat", st.name))...))
				}
			case metrics.Counter:
				o.ObserveInt64(total, m.Count(), metric.WithAttributes(attrs...))
			case metrics.Gauge:
				o.ObserveFloat64(value, float64(m.Value()), metric.WithAttributes(attrs...))
			case metrics.GaugeFloat64:
				o.ObserveFloat64(value, m.Value(), metric.WithAttributes(attrs...))
			}
		})
		return nil
	}, rate, total, dist, value)
	if err != nil {
		return fmt.Errorf("logger: kafka metrics callback: %w", err)
	}
	return nil
}

// saramaMetricAttrs splits "request-latency-in-ms-for-broker-1" into
// name=request-latency-in-ms, broker=1 (same for "-for-topic-<t>").
func saramaMetricAttrs(name string) []attribute.KeyValue {
	if before, after, ok := strings.Cut(name, "-for-broker-"); ok {
		return []attribute.KeyValue{
			attribute.String("metric", before),
			attribute.String("messaging.kafka.broker", after),
		}
	}
	if before, after, ok := strings.Cut(name, "-for-topic-"); ok {
		return []attribute.KeyValue{
			attribute.String("metric", before),
			attribute.String("messaging.destination.name", after),
		}
	}
	return []attribute.KeyValue{attribute.String("metric", name)}
}

// InjectBrokerSyncProducer traces every SendMessage(s). cfg must be the config
// the producer was built with (it decides whether headers can carry the trace).
func (l *loggerServiceImpl) InjectBrokerSyncProducer(cfg *sarama.Config, p sarama.SyncProducer) (sarama.SyncProducer, error) {
	if err := l.requireStarted(); err != nil {
		return nil, err
	}
	if cfg == nil || p == nil {
		return nil, ErrNilTarget
	}
	return otelsarama.WrapSyncProducer(cfg, p), nil
}

// InjectBrokerAsyncProducer traces messages sent through Input(). Read
// Successes()/Errors() from the returned producer, not the original one.
func (l *loggerServiceImpl) InjectBrokerAsyncProducer(cfg *sarama.Config, p sarama.AsyncProducer) (sarama.AsyncProducer, error) {
	if err := l.requireStarted(); err != nil {
		return nil, err
	}
	if cfg == nil || p == nil {
		return nil, ErrNilTarget
	}
	return otelsarama.WrapAsyncProducer(cfg, p), nil
}

// InjectBrokerConsumerGroup traces every message delivered to h.ConsumeClaim.
func (l *loggerServiceImpl) InjectBrokerConsumerGroup(h sarama.ConsumerGroupHandler) (sarama.ConsumerGroupHandler, error) {
	if err := l.requireStarted(); err != nil {
		return nil, err
	}
	if h == nil {
		return nil, ErrNilTarget
	}
	return otelsarama.WrapConsumerGroupHandler(h), nil
}

// InjectBrokerConsumer traces messages from every partition consumer it creates.
func (l *loggerServiceImpl) InjectBrokerConsumer(c sarama.Consumer) (sarama.Consumer, error) {
	if err := l.requireStarted(); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, ErrNilTarget
	}
	return otelsarama.WrapConsumer(c), nil
}

// InjectBrokerPartitionConsumer traces messages from a single partition consumer.
func (l *loggerServiceImpl) InjectBrokerPartitionConsumer(pc sarama.PartitionConsumer) (sarama.PartitionConsumer, error) {
	if err := l.requireStarted(); err != nil {
		return nil, err
	}
	if pc == nil {
		return nil, ErrNilTarget
	}
	return otelsarama.WrapPartitionConsumer(pc), nil
}

// BrokerMessageWithContext implements [LoggerService].
func (l *loggerServiceImpl) BrokerMessageWithContext(ctx context.Context, msg *sarama.ProducerMessage) {
	if ctx == nil || msg == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, otelsarama.NewProducerMessageCarrier(msg))
}

// BrokerMessageContext implements [LoggerService].
func (l *loggerServiceImpl) BrokerMessageContext(ctx context.Context, msg *sarama.ConsumerMessage) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if msg == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, otelsarama.NewConsumerMessageCarrier(msg))
}
