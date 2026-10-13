package broker

import (
	"context"
	"errors"
	"strconv"
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

	dlqSuffix string
	attempts  int
	backoff   time.Duration

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
	brokers []string, groupID string, inflight *utils.Inflight) IBrokerService {
	return &BrokerService{
		brokers:   brokers,
		groupID:   groupID,
		gate:      utils.NewGate(inflight),
		subs:      map[string]*subscription{},
		dlqSuffix: DLQSuffix,
		attempts:  HandlerAttempts,
		backoff:   RetryBackoff,
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
	// Open before consuming: a backlog is handled the moment a consumer
	// starts, and a closed gate would end its loop.
	b.producer = producer
	b.gate.Open()
	for _, s := range b.subs {
		if err := b.consume(s); err != nil {
			b.closeLocked()
			_ = b.gate.Close(context.Background())
			return err
		}
	}
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

// loop hands each message to handler and commits what was handled or
// parked. When the service stops mid-batch the rest stays uncommitted, to be
// delivered again.
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

// handle runs handler on r, retrying with backoff, then parks r on its
// dead-letter topic if it never succeeds. It reports false when the service
// is stopping and r is neither handled nor parked, so it is not committed.
func (b *BrokerService) handle(handler Handler, r *kgo.Record) bool {
	done, ok := b.gate.Enter()
	if !ok {
		return false
	}
	defer done()
	wait := b.backoff
	var err error
	for attempt := 1; attempt <= b.attempts; attempt++ {
		if err = safely(handler, r); err == nil {
			return true
		}
		if attempt < b.attempts {
			time.Sleep(wait)
			wait *= 2
		}
	}
	return b.deadLetter(r, err)
}

// deadLetter publishes r to <topic><suffix> and waits until Kafka has it.
// While the DLQ is unreachable it keeps trying rather than dropping r; it
// gives up (false, r stays uncommitted) only when the service stops.
func (b *BrokerService) deadLetter(r *kgo.Record, cause error) bool {
	b.mu.Lock()
	producer := b.producer
	b.mu.Unlock()
	if producer == nil {
		return false
	}
	dead := &kgo.Record{
		Topic: r.Topic + b.dlqSuffix,
		Key:   r.Key,
		Value: r.Value,
		Headers: append(append([]kgo.RecordHeader(nil), r.Headers...),
			kgo.RecordHeader{Key: HeaderDLQTopic, Value: []byte(r.Topic)},
			kgo.RecordHeader{Key: HeaderDLQPartition, Value: []byte(strconv.Itoa(int(r.Partition)))},
			kgo.RecordHeader{Key: HeaderDLQOffset, Value: []byte(strconv.FormatInt(r.Offset, 10))},
			kgo.RecordHeader{Key: HeaderDLQError, Value: []byte(cause.Error())},
			kgo.RecordHeader{Key: HeaderDLQAttempts, Value: []byte(strconv.Itoa(b.attempts))},
			kgo.RecordHeader{Key: HeaderDLQGroup, Value: []byte(b.groupID)},
			kgo.RecordHeader{Key: HeaderDLQFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		),
	}
	wait := b.backoff
	for b.gate.IsOpen() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := producer.ProduceSync(ctx, dead).FirstErr()
		cancel()
		if err == nil {
			return true
		}
		time.Sleep(wait)
		wait = min(wait*2, maxDLQBackoff)
	}
	return false
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
