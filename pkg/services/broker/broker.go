package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

const commitTimeout = 5 * time.Second

type MessageBrokerService struct {
	brokers []string
	groupID string
	opts    Options
	log     logger.LogContextService

	publishLimiter *rate.Limiter
	consumeLimiter *rate.Limiter

	mu       sync.RWMutex
	producer *kgo.Client
	queue    chan *kgo.Record // Enqueue's buffer, read by the batching loop
	stopCh   chan struct{}    // closed by Stop to release senders blocked on a full queue
	inflight sync.WaitGroup   // Enqueue calls that have not finished handing over their message
	flushReq chan chan struct{}
	loopDone chan struct{}
	cancel   context.CancelFunc
}

func NewBrokerService(brokers []string, groupID string, opts Options, log logger.LogContextService) BatchBrokerServices {
	opts = opts.withDefaults()
	m := &MessageBrokerService{brokers: brokers, groupID: groupID, opts: opts, log: log}
	if opts.Producer.Rate > 0 {
		m.publishLimiter = rate.NewLimiter(rate.Limit(opts.Producer.Rate), opts.Producer.Burst)
	}
	if opts.Consumer.Rate > 0 {
		m.consumeLimiter = rate.NewLimiter(rate.Limit(opts.Consumer.Rate), opts.Consumer.Burst)
	}
	return m
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

func (m *MessageBrokerService) observe(name, topic string, fn func() error, kv ...any) error {
	if m.log == nil {
		return fn()
	}
	_, lvl := m.log.Trace(name, attribute.String("broker.topic", topic))
	defer lvl.Span().End()
	kv = append([]any{"component", "broker", "topic", topic}, kv...)
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
	m.startLoop()
	m.emit("broker.ready", func(l logger.LoggerLevel) {
		p, c := m.opts.Producer, m.opts.Consumer
		l.Info("broker ready", "component", "broker", "brokers", m.brokers, "group", m.groupID,
			"producer_batch_size", p.BatchSize, "producer_batch_wait", p.BatchWait.String(),
			"producer_max_pending", p.MaxPending, "producer_rate", p.Rate,
			"consumer_batch_size", c.BatchSize, "consumer_batch_wait", c.BatchWait.String(), "consumer_rate", c.Rate)
	})
	return nil
}

func (m *MessageBrokerService) Stop(ctx context.Context) error {
	m.mu.Lock()
	producer, queue, stopCh, loopDone, cancel := m.producer, m.queue, m.stopCh, m.loopDone, m.cancel
	m.producer, m.queue, m.stopCh, m.flushReq, m.loopDone, m.cancel = nil, nil, nil, nil, nil, nil
	m.mu.Unlock()
	if producer == nil {
		return nil
	}
	close(stopCh)
	m.inflight.Wait()
	close(queue)
	select {
	case <-loopDone:
	case <-ctx.Done():
		cancel()
		<-loopDone
	}
	cancel()
	producer.Close()
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
		if err := m.waitPublish(ctx, 1); err != nil {
			return err
		}
		rec := &kgo.Record{Topic: topic, Key: key, Value: value}
		if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
			return fmt.Errorf("publishing to %s: %w", topic, err)
		}
		return nil
	})
}

// waitPublish takes n tokens from the publish rate limiter; n must not exceed its burst.
func (m *MessageBrokerService) waitPublish(ctx context.Context, n int) error {
	if m.publishLimiter == nil {
		return nil
	}
	if err := m.publishLimiter.WaitN(ctx, n); err != nil {
		return fmt.Errorf("waiting for the publish rate limit: %w", err)
	}
	return nil
}

// Subscribe delivers a topic's records to handler, one at a time and in order per partition, until ctx
// ends (it then returns nil). A record is committed only after handler returns nil, so delivery is at
// least once: if handler fails, Subscribe commits what came before, stops and returns the error, and the
// failed record is delivered again to the next subscriber in the group.
func (m *MessageBrokerService) Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error {
	client, err := m.consumer(topic)
	if err != nil {
		return err
	}
	defer client.Close()

	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		m.logFetchErrors(fetches)

		var handled []*kgo.Record
		for _, rec := range fetches.Records() {
			if m.consumeLimiter != nil {
				if err := m.consumeLimiter.Wait(ctx); err != nil {
					m.commit(ctx, client, topic, handled)
					return nil // the context ended while waiting
				}
			}
			if err := handler(rec.Key, rec.Value); err != nil {
				m.commit(ctx, client, topic, handled)
				return m.handlerFailed(rec, err)
			}
			handled = append(handled, rec)
		}
		m.commit(ctx, client, topic, handled)
	}
}

// consumer joins the group for topic with manual commits, starting from the beginning for a new group.
func (m *MessageBrokerService) consumer(topic string) (*kgo.Client, error) {
	if m.groupID == "" {
		return nil, ErrNoGroup
	}
	client, err := m.newClient(
		kgo.ConsumerGroup(m.groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("creating kafka consumer for %s: %w", topic, err)
	}
	m.emit("broker.subscribe", func(l logger.LoggerLevel) {
		l.Info("broker subscribed", "component", "broker", "topic", topic, "group", m.groupID)
	})
	return client, nil
}

func (m *MessageBrokerService) logFetchErrors(fetches kgo.Fetches) {
	fetches.EachError(func(t string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return // a poll that ended on purpose
		}
		m.emit("broker.fetch", func(l logger.LoggerLevel) {
			l.Warn("broker fetch failed", "component", "broker", "topic", t, "partition", partition, "error", err.Error())
		})
	})
}

func (m *MessageBrokerService) handlerFailed(rec *kgo.Record, err error) error {
	err = fmt.Errorf("handler failed on %s[%d]@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
	m.emit("broker.handler", func(l logger.LoggerLevel) {
		l.Error(err, "broker handler failed",
			"component", "broker", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset)
	})
	return err
}

// commit records progress even when ctx has just ended, so a clean shutdown does not redeliver.
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
