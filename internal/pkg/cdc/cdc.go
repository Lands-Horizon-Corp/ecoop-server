package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// CDCService reads Debezium change events from Kafka. It owns everything
// between Kafka and the handler: connection, decoding, retries, the
// dead-letter topic and offset commits.
type CDCService struct {
	brokers []string
	groupID string
	prefix  string
	gate    *utils.Gate

	dlqSuffix string
	attempts  int
	backoff   time.Duration

	mu       sync.Mutex
	producer *kgo.Client // writes dead letters
	subs     map[string]*subscription
}

type subscription struct {
	topic   string
	handler Handler
	client  *kgo.Client // nil while not running
}

// NewCDCService creates the service. brokers are Kafka's addresses,
// groupID prefixes each topic's consumer group, and topicPrefix is the
// Debezium connector's topic.prefix (empty means "ecoop"). Run reports
// missing addresses; Subscribe a missing group.
func NewCDCService(brokers []string, groupID, topicPrefix string) CDCServices {
	if strings.TrimSpace(topicPrefix) == "" {
		topicPrefix = defaultTopicPrefix
	}
	return &CDCService{
		brokers:   brokers,
		groupID:   strings.TrimSpace(groupID),
		prefix:    topicPrefix,
		gate:      utils.NewGate(nil),
		dlqSuffix: defaultDLQSuffix,
		attempts:  defaultAttempts,
		backoff:   defaultBackoff,
		subs:      map[string]*subscription{},
	}
}

func (c *CDCService) Run(ctx context.Context) error {
	if c == nil || c.gate == nil || c.subs == nil {
		return ErrNoService
	}
	if len(c.brokers) == 0 {
		return ErrNoBrokers
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.producer != nil {
		return nil
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(c.brokers...), kgo.AllowAutoTopicCreation())
	if err != nil {
		return errors.Join(ErrConnect, err)
	}
	if err := producer.Ping(ctx); err != nil {
		producer.Close()
		return errors.Join(ErrConnect, err)
	}
	// Open before consuming: a backlog is handled the moment a consumer
	// starts, and a closed gate would end its loop.
	c.producer = producer
	c.gate.Open()
	for _, s := range c.subs {
		if err := c.consume(s); err != nil {
			c.closeLocked()
			_ = c.gate.Close(context.Background())
			return err
		}
	}
	return nil
}

func (c *CDCService) Stop(ctx context.Context) error {
	if c == nil || c.gate == nil {
		return nil
	}
	err := c.gate.Close(ctx) // no new changes; running handlers finish
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
	return err
}

// closeLocked disconnects every client; the caller holds mu.
func (c *CDCService) closeLocked() {
	for _, s := range c.subs {
		if s.client != nil {
			s.client.Close()
			s.client = nil
		}
	}
	if c.producer != nil {
		c.producer.Close()
		c.producer = nil
	}
}

func (c *CDCService) Topic(schema, table string) string {
	if c == nil {
		return ""
	}
	return c.prefix + "." + schema + "." + table
}

func (c *CDCService) Subscribe(ctx context.Context, topic string, handler Handler) error {
	if c == nil || c.subs == nil {
		return ErrNoService
	}
	var errs []error
	if strings.TrimSpace(topic) == "" {
		errs = append(errs, ErrNoTopic)
	}
	if handler == nil {
		errs = append(errs, ErrNoHandler)
	}
	if c.groupID == "" {
		errs = append(errs, ErrNoGroup)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.subs[topic]; ok {
		return ErrDuplicateSubscription
	}
	s := &subscription{topic: topic, handler: handler}
	if c.producer != nil { // running: start now
		if err := c.consume(s); err != nil {
			return err
		}
	}
	c.subs[topic] = s
	return nil
}

// group is the consumer group of topic: one per topic, so a topic's offsets
// move independently, while every instance consuming it shares them.
func (c *CDCService) group(topic string) string {
	return c.groupID + "." + topic
}

// consume starts s's consumer; the caller holds mu. A new group starts from
// the topic's beginning, so nothing published before it joined is missed.
func (c *CDCService) consume(s *subscription) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(c.brokers...),
		kgo.ConsumerGroup(c.group(s.topic)),
		kgo.ConsumeTopics(s.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return errors.Join(ErrConnect, err)
	}
	s.client = client
	go c.loop(client, s.handler)
	return nil
}

// loop handles each record in order and commits it once handled or parked.
// When the service stops mid-batch the rest stays uncommitted, to be
// delivered again.
func (c *CDCService) loop(client *kgo.Client, handler Handler) {
	for {
		fetches := client.PollFetches(context.Background())
		if fetches.IsClientClosed() {
			return
		}
		stopped := false
		fetches.EachRecord(func(r *kgo.Record) {
			if stopped {
				return
			}
			if !c.handle(handler, r) {
				stopped = true
				return
			}
			// One commit per record: a crash replays at most this one.
			_ = client.CommitRecords(context.Background(), r)
		})
		if stopped {
			return
		}
	}
}

// handle decodes r and runs handler, retrying with backoff, then parks r on
// its dead-letter topic if it never succeeds. It reports false when the
// service is stopping and r is neither handled nor parked, so it is not
// committed.
func (c *CDCService) handle(handler Handler, r *kgo.Record) bool {
	done, ok := c.gate.Enter()
	if !ok {
		return false
	}
	defer done()
	wait := c.backoff
	var err error
	for attempt := 1; attempt <= c.attempts; attempt++ {
		if err = apply(handler, r); err == nil {
			return true
		}
		if attempt < c.attempts {
			time.Sleep(wait)
			wait *= 2
		}
	}
	return c.deadLetter(r, err)
}

// apply decodes r and calls handler, turning a panic into an error so one
// bad change cannot crash the consumer. A tombstone is skipped.
func apply(handler Handler, r *kgo.Record) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = errors.New("cdc: handler panicked")
		}
	}()
	event, err := Decode(r.Value)
	if err != nil {
		return err
	}
	if event == nil { // tombstone
		return nil
	}
	return handler(context.Background(), json.RawMessage(r.Key), *event)
}

// deadLetter publishes r to <topic><suffix> and waits until Kafka has it.
// While the DLQ is unreachable it keeps trying rather than dropping r; it
// gives up (false, r stays uncommitted) only when the service stops.
func (c *CDCService) deadLetter(r *kgo.Record, cause error) bool {
	c.mu.Lock()
	producer := c.producer
	c.mu.Unlock()
	if producer == nil {
		return false
	}
	dead := &kgo.Record{
		Topic: r.Topic + c.dlqSuffix,
		Key:   r.Key,
		Value: r.Value,
		Headers: append(append([]kgo.RecordHeader(nil), r.Headers...),
			kgo.RecordHeader{Key: HeaderDLQTopic, Value: []byte(r.Topic)},
			kgo.RecordHeader{Key: HeaderDLQPartition, Value: []byte(strconv.Itoa(int(r.Partition)))},
			kgo.RecordHeader{Key: HeaderDLQOffset, Value: []byte(strconv.FormatInt(r.Offset, 10))},
			kgo.RecordHeader{Key: HeaderDLQError, Value: []byte(cause.Error())},
			kgo.RecordHeader{Key: HeaderDLQAttempts, Value: []byte(strconv.Itoa(c.attempts))},
			kgo.RecordHeader{Key: HeaderDLQGroup, Value: []byte(c.group(r.Topic))},
			kgo.RecordHeader{Key: HeaderDLQFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		),
	}
	wait := c.backoff
	for c.gate.IsOpen() {
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

// Decode parses a Debezium JSON value, with or without the
// {"schema":…,"payload":…} wrapper. An empty value is the tombstone sent
// after a delete: it returns (nil, nil). Anything else that is not a change
// event returns ErrNotChangeEvent.
func Decode(value []byte) (*ChangeEvent, error) {
	if len(value) == 0 {
		return nil, nil
	}
	var wrapped struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(value, &wrapped); err != nil {
		return nil, errors.Join(ErrNotChangeEvent, err)
	}
	if len(wrapped.Payload) > 0 {
		value = wrapped.Payload
	}
	var event ChangeEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return nil, errors.Join(ErrNotChangeEvent, err)
	}
	if event.Op == "" {
		return nil, ErrNotChangeEvent
	}
	return &event, nil
}
