package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
)

const commitTimeout = 5 * time.Second

type MessageBrokerService struct {
	brokers []string
	groupID string
	log     logger.LogContextService

	mu       sync.RWMutex
	producer *kgo.Client
}

// NewBrokerService builds a Kafka-backed broker. groupID is the consumer group every Subscribe joins, so
// instances of one service share a topic's messages. log is optional (it must already be started).
func NewBrokerService(brokers []string, groupID string, log logger.LogContextService) MessageBrokerServices {
	return &MessageBrokerService{brokers: brokers, groupID: groupID, log: log}
}

func (m *MessageBrokerService) newClient(extra ...kgo.Opt) (*kgo.Client, error) {
	if len(m.brokers) == 0 {
		return nil, ErrNoBrokers
	}
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(m.brokers...),
		kgo.AllowAutoTopicCreation(),
	}, extra...)
	return kgo.NewClient(opts...)
}

func (m *MessageBrokerService) observe(name, topic string, fn func() error) error {
	if m.log == nil {
		return fn()
	}
	_, lvl := m.log.Trace(name, attribute.String("broker.topic", topic))
	defer lvl.Span().End()
	kv := []any{"component", "broker", "topic", topic}
	if err := fn(); err != nil {
		lvl.Error(err, name+" failed", kv...)
		return err
	}
	lvl.Debug(name+" done", kv...)
	return nil
}

func (m *MessageBrokerService) emit(name string, write func(logger.LoggerLevel)) {
	if m.log != nil {
		m.log.Emit(name, write)
	}
}

// Run connects the producer and checks the brokers answer. It is a no-op when already running.
func (m *MessageBrokerService) Run(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.producer != nil {
		return nil
	}
	err := m.observe("broker.run", "", func() error {
		client, err := m.newClient(kgo.RequiredAcks(kgo.AllISRAcks()))
		if err != nil {
			return fmt.Errorf("creating kafka client: %w", err)
		}
		if err := client.Ping(ctx); err != nil {
			client.Close()
			return fmt.Errorf("kafka brokers %v are not reachable: %w", m.brokers, err)
		}
		m.producer = client
		return nil
	})
	if err != nil {
		return err
	}
	m.emit("broker.ready", func(l logger.LoggerLevel) {
		l.Info("broker ready", "component", "broker", "brokers", m.brokers, "group", m.groupID)
	})
	return nil
}

func (m *MessageBrokerService) Stop(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.producer == nil {
		return nil
	}
	m.producer.Close()
	m.producer = nil
	m.emit("broker.stopped", func(l logger.LoggerLevel) {
		l.Info("broker stopped", "component", "broker")
	})
	return nil
}

func (m *MessageBrokerService) Publish(ctx context.Context, topic string, key, value []byte) error {
	m.mu.RLock()
	producer := m.producer
	m.mu.RUnlock()
	if producer == nil {
		return ErrNotRunning
	}
	return m.observe("broker.publish", topic, func() error {
		rec := &kgo.Record{Topic: topic, Key: key, Value: value}
		if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
			return fmt.Errorf("publishing to %s: %w", topic, err)
		}
		return nil
	})
}

func (m *MessageBrokerService) Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error {
	if m.groupID == "" {
		return ErrNoGroup
	}
	client, err := m.newClient(
		kgo.ConsumerGroup(m.groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return fmt.Errorf("creating kafka consumer for %s: %w", topic, err)
	}
	defer client.Close()
	m.emit("broker.subscribe", func(l logger.LoggerLevel) {
		l.Info("broker subscribed", "component", "broker", "topic", topic, "group", m.groupID)
	})

	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, partition int32, err error) {
			m.emit("broker.fetch", func(l logger.LoggerLevel) {
				l.Warn("broker fetch failed", "component", "broker", "topic", t, "partition", partition, "error", err.Error())
			})
		})

		var handled []*kgo.Record
		for _, rec := range fetches.Records() {
			if err := handler(rec.Key, rec.Value); err != nil {
				m.commit(ctx, client, topic, handled)
				err = fmt.Errorf("handler failed on %s[%d]@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
				m.emit("broker.handler", func(l logger.LoggerLevel) {
					l.Error(err, "broker handler failed",
						"component", "broker", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset)
				})
				return err
			}
			handled = append(handled, rec)
		}
		m.commit(ctx, client, topic, handled)
	}
}

func (m *MessageBrokerService) commit(ctx context.Context, client *kgo.Client, topic string, recs []*kgo.Record) {
	if len(recs) == 0 {
		return
	}
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()
	if err := client.CommitRecords(commitCtx, recs...); err != nil {
		m.emit("broker.commit", func(l logger.LoggerLevel) {
			l.Error(err, "broker commit failed", "component", "broker", "topic", topic, "records", len(recs))
		})
	}
}
