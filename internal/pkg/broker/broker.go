package broker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// BrokerService is a Kafka producer and consumer group.
type BrokerService struct {
	brokers []string
	groupID string
	gate    *utils.Gate

	mu       sync.Mutex
	producer *kgo.Client
	subs     map[string]*subscription
}

type subscription struct {
	topic   string
	handler Handler
	client  *kgo.Client // nil while not running
}

// NewBrokerService creates the service. groupID is only needed to
// Subscribe. inflight may be shared with other services; nil gives the
// service its own tracker.
func NewBrokerService(
	brokers []string, groupID string, inflight *utils.Inflight) BrokerServices {
	return &BrokerService{
		brokers: brokers,
		groupID: groupID,
		gate:    utils.NewGate(inflight),
		subs:    map[string]*subscription{},
	}
}

func (b *BrokerService) Run(ctx context.Context) error {
	if b.gate == nil || b.subs == nil {
		return ErrNoTracker
	}
	if len(b.brokers) == 0 {
		return ErrNoBrokers
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.producer != nil {
		return nil
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(b.brokers...), kgo.AllowAutoTopicCreation())
	if err != nil {
		return errors.Join(ErrConnect, err)
	}
	if err := producer.Ping(ctx); err != nil {
		producer.Close()
		return errors.Join(ErrConnect, err)
	}
	for _, s := range b.subs {
		if err := b.consume(s); err != nil {
			b.closeLocked()
			producer.Close()
			return err
		}
	}
	b.producer = producer
	b.gate.Open()
	return nil
}

func (b *BrokerService) Stop(ctx context.Context) error {
	err := b.gate.Close(ctx) // no new messages; running handlers finish
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closeLocked()
	return err
}

// closeLocked disconnects every client; the caller holds mu.
func (b *BrokerService) closeLocked() {
	for _, s := range b.subs {
		if s.client != nil {
			s.client.Close()
			s.client = nil
		}
	}
	if b.producer != nil {
		b.producer.Close()
		b.producer = nil
	}
}

func (b *BrokerService) Publish(ctx context.Context, topic string, key, value []byte) error {
	if strings.TrimSpace(topic) == "" {
		return ErrNoTopic
	}
	done, ok := b.gate.Enter()
	if !ok {
		return ErrNotRunning
	}
	defer done()
	b.mu.Lock()
	producer := b.producer
	b.mu.Unlock()
	if producer == nil {
		return ErrNotRunning
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).FirstErr(); err != nil {
		return errors.Join(ErrPublish, err)
	}
	return nil
}

func (b *BrokerService) Subscribe(ctx context.Context, topic string, handler func(ctx context.Context, key, value []byte) error) error {
	if b.subs == nil {
		return ErrNoTracker
	}
	var errs []error
	if strings.TrimSpace(topic) == "" {
		errs = append(errs, ErrNoTopic)
	}
	if handler == nil {
		errs = append(errs, ErrNoHandler)
	}
	if b.groupID == "" {
		errs = append(errs, ErrNoGroup)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subs[topic]; ok {
		return ErrDuplicateSubscription
	}
	s := &subscription{topic: topic, handler: handler}
	if b.producer != nil { // running: start now
		if err := b.consume(s); err != nil {
			return err
		}
	}
	b.subs[topic] = s
	return nil
}

// consume starts s's consumer; the caller holds mu. A new group starts from
// the topic's beginning, so nothing published before it joined is missed.
func (b *BrokerService) consume(s *subscription) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(b.brokers...),
		kgo.ConsumerGroup(b.groupID),
		kgo.ConsumeTopics(s.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return errors.Join(ErrConnect, err)
	}
	s.client = client
	go b.loop(client, s.handler)
	return nil
}

// loop hands each message to handler and commits what was handled. When
// the service stops mid-batch the rest stays uncommitted, to be delivered
// again.
func (b *BrokerService) loop(client *kgo.Client, handler Handler) {
	for {
		fetches := client.PollFetches(context.Background())
		if fetches.IsClientClosed() {
			return
		}
		var handled []*kgo.Record
		stopped := false
		fetches.EachRecord(func(r *kgo.Record) {
			if stopped {
				return
			}
			if !b.handle(handler, r) {
				stopped = true
				return
			}
			handled = append(handled, r)
		})
		if len(handled) > 0 {
			_ = client.CommitRecords(context.Background(), handled...)
		}
		if stopped {
			return
		}
	}
}

// handle runs handler on r, retrying with backoff. It reports false when
// the service is stopping and r was not handled.
func (b *BrokerService) handle(handler Handler, r *kgo.Record) bool {
	done, ok := b.gate.Enter()
	if !ok {
		return false
	}
	defer done()
	wait := RetryBackoff
	for attempt := 1; attempt <= HandlerAttempts; attempt++ {
		if err := safely(handler, r); err == nil || attempt == HandlerAttempts {
			return true // done, or given up on: skipped so the topic moves on
		}
		time.Sleep(wait)
		wait *= 2
	}
	return true
}

// safely runs handler, turning a panic into an error so one bad message
// cannot crash the consumer.
func safely(handler Handler, r *kgo.Record) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = errors.New("broker: handler panicked")
		}
	}()
	return handler(context.Background(), r.Key, r.Value)
}
