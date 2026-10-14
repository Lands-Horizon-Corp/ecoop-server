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

type CDCService struct {
	brokers []string
	groupID string
	prefix  string
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
	client  *kgo.Client
}

func NewCDCService(brokers []string, groupID, topicPrefix string) ICDCService {
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
	err := c.gate.Close(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
	return err
}

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
	if c.producer != nil {
		if err := c.consume(s); err != nil {
			return err
		}
	}
	c.subs[topic] = s
	return nil
}

func (c *CDCService) group(topic string) string {
	return c.groupID + "." + topic
}

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
			if err := client.CommitRecords(context.Background(), r); err != nil {
				stopped = true
				return
			}
		})
		if stopped {
			return
		}
	}
}

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

func apply(handler Handler, r *kgo.Record) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = errors.New("cdc: handler panicked")
		}
	}()
	event, err := utils.Decode[ChangeEvent](r.Value)
	if err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	return handler(context.Background(), json.RawMessage(r.Key), *event)
}

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
