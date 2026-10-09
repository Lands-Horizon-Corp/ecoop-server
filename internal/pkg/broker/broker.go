package broker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/logger"
)

// BrokerService is a Kafka-backed MessageBrokerServices. Delivery is
// at-least-once: a message is committed only after its handler succeeds or it
// has been written to "<topic>.dlq". Handlers must therefore be idempotent.
type BrokerService struct {
	brokers      []string
	groupID      string
	log          logger.LoggerService
	cfg          *sarama.Config
	maxRetries   int
	retryBackoff time.Duration

	mu       sync.RWMutex
	running  bool
	producer sarama.SyncProducer
	subs     map[string]Handler
	groups   []sarama.ConsumerGroup
	runCtx   context.Context
	cancel   context.CancelFunc
	loops    sync.WaitGroup
	inflight sync.WaitGroup

	sent     metric.Int64Counter
	consumed metric.Int64Counter
	process  metric.Float64Histogram
}

// NewBrokerService builds a broker for the given Kafka brokers. groupID is the
// consumer group shared by every Subscribe of this service (use one per
// service, e.g. "ecoop-server"). log must be started before Run.
func NewBrokerService(brokers []string, groupID string, log logger.LoggerService, opts ...Option) MessageBrokerServices {
	s := &BrokerService{
		brokers:      brokers,
		groupID:      groupID,
		log:          log,
		cfg:          NewSaramaConfig(groupID),
		maxRetries:   defaultMaxRetries,
		retryBackoff: defaultRetryBackoff,
		subs:         map[string]Handler{},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Run implements [MessageBrokerServices].
func (s *BrokerService) Run(ctx context.Context) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return ErrAlreadyRunning
	}
	if s.log == nil {
		return fmt.Errorf("%w: logger is nil", ErrInvalidArgument)
	}
	if len(s.brokers) == 0 || s.groupID == "" {
		return fmt.Errorf("%w: brokers and group id are required", ErrInvalidArgument)
	}
	if s.maxRetries < 0 || s.retryBackoff < 0 {
		return fmt.Errorf("%w: retries and backoff must not be negative", ErrInvalidArgument)
	}
	if err := s.cfg.Validate(); err != nil {
		return fmt.Errorf("broker: config: %w", err)
	}
	if err := s.log.InjectBrokerConfig(s.cfg); err != nil {
		return fmt.Errorf("broker: instrument config: %w", err)
	}
	if err := s.initMetrics(); err != nil {
		return err
	}
	raw, err := sarama.NewSyncProducer(s.brokers, s.cfg)
	if err != nil {
		return fmt.Errorf("broker: producer: %w", err)
	}
	if s.producer, err = s.log.InjectBrokerSyncProducer(s.cfg, raw); err != nil {
		return errors.Join(fmt.Errorf("broker: instrument producer: %w", err), raw.Close())
	}
	s.runCtx, s.cancel = context.WithCancel(trace.ContextWithSpanContext(context.WithoutCancel(ctx), trace.SpanContext{}))
	defer func() {
		if err != nil {
			err = errors.Join(err, s.teardownLocked())
		}
	}()
	for topic, h := range s.subs {
		if err := s.startConsumerLocked(topic, h); err != nil {
			return err
		}
	}
	s.running = true
	return nil
}

func (s *BrokerService) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return ErrNotRunning
	}
	s.running = false
	s.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		done <- s.teardownLocked()
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("broker: stop: %w", ctx.Err())
	}
}

func (s *BrokerService) teardownLocked() error {
	var errs []error
	if s.cancel != nil {
		s.cancel()
	}
	for _, g := range s.groups {
		if err := g.Close(); err != nil {
			errs = append(errs, fmt.Errorf("broker: close consumer: %w", err))
		}
	}
	s.groups = nil
	s.loops.Wait()
	s.inflight.Wait()
	if s.producer != nil {
		if err := s.producer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("broker: close producer: %w", err))
		}
		s.producer = nil
	}
	return errors.Join(errs...)
}

func (s *BrokerService) Publish(ctx context.Context, topic string, key, value []byte) error {
	if topic == "" {
		return fmt.Errorf("%w: empty topic", ErrInvalidArgument)
	}
	s.mu.RLock()
	if !s.running {
		s.mu.RUnlock()
		return ErrNotRunning
	}
	s.inflight.Add(1)
	s.mu.RUnlock()
	defer s.inflight.Done()

	return s.send(ctx, &sarama.ProducerMessage{Topic: topic, Key: encoder(key), Value: encoder(value)})
}

func (s *BrokerService) send(ctx context.Context, msg *sarama.ProducerMessage) error {
	s.log.BrokerMessageWithContext(ctx, msg)
	_, _, err := s.producer.SendMessage(msg)
	s.sent.Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.destination.name", msg.Topic),
		attribute.String("outcome", outcome(err)),
	))
	if err != nil {
		return fmt.Errorf("broker: publish to %s: %w", msg.Topic, err)
	}
	return nil
}

func (s *BrokerService) Subscribe(ctx context.Context, topic string, handler Handler) error {
	if topic == "" || handler == nil {
		return fmt.Errorf("%w: topic and handler are required", ErrInvalidArgument)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subs[topic]; ok {
		return fmt.Errorf("%w: %s", ErrDuplicateSubscription, topic)
	}
	if s.running {
		if err := s.startConsumerLocked(topic, handler); err != nil {
			return err
		}
	}
	s.subs[topic] = handler
	return nil
}

func (s *BrokerService) startConsumerLocked(topic string, h Handler) error {
	group, err := sarama.NewConsumerGroup(s.brokers, s.groupID, s.cfg)
	if err != nil {
		return fmt.Errorf("broker: consumer for %s: %w", topic, err)
	}
	wrapped, err := s.log.InjectBrokerConsumerGroup(&groupHandler{svc: s, topic: topic, handle: h})
	if err != nil {
		return errors.Join(fmt.Errorf("broker: instrument consumer for %s: %w", topic, err), group.Close())
	}
	s.groups = append(s.groups, group)

	ctx := s.runCtx
	s.loops.Add(2)
	go func() {
		defer s.loops.Done()
		for err := range group.Errors() {
			s.logError(ctx, "kafka.consumer.error", err, "consumer error", "topic", topic)
		}
	}()
	go func() {
		defer s.loops.Done()
		for ctx.Err() == nil {
			err := group.Consume(ctx, []string{topic}, wrapped)
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
			if err != nil {
				s.logError(ctx, "kafka.consume", err, "consume failed, retrying", "topic", topic)
				select {
				case <-ctx.Done():
				case <-time.After(reconnectBackoff):
				}
			}
		}
	}()
	return nil
}

func (s *BrokerService) logError(ctx context.Context, span string, err error, msg string, kv ...any) {
	_, l := s.log.NewContext(ctx).Trace(span)
	l.Error(err, msg, kv...)
	l.Span().End()
}

func (s *BrokerService) initMetrics() error {
	m := s.log.Meter()
	var err error
	if s.sent, err = m.Int64Counter("messaging.client.sent.messages",
		metric.WithDescription("Messages published, by topic and outcome.")); err != nil {
		return fmt.Errorf("broker: metric: %w", err)
	}
	if s.consumed, err = m.Int64Counter("messaging.client.consumed.messages",
		metric.WithDescription("Messages consumed, by topic and outcome (success, retried, dead_letter).")); err != nil {
		return fmt.Errorf("broker: metric: %w", err)
	}
	if s.process, err = m.Float64Histogram("messaging.process.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Time spent handling a message, including retries.")); err != nil {
		return fmt.Errorf("broker: metric: %w", err)
	}
	return nil
}

type groupHandler struct {
	svc    *BrokerService
	topic  string
	handle Handler
}

func (g *groupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (g *groupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (g *groupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-sess.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if err := g.processMessage(sess.Context(), msg); err != nil {
				return err
			}
			sess.MarkMessage(msg, "")
		}
	}
}
func (g *groupHandler) processMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	s := g.svc
	start := time.Now()
	ctx = s.log.BrokerMessageContext(ctx, msg)
	logCtx, log := s.log.NewContext(ctx).Trace("kafka.process "+g.topic,
		attribute.String("messaging.destination.name", msg.Topic),
		attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
		attribute.Int64("messaging.kafka.offset", msg.Offset),
		attribute.String("messaging.kafka.consumer.group", s.groupID),
	)
	defer log.Span().End()

	result := "success"
	defer func() {
		set := metric.WithAttributes(attribute.String("messaging.destination.name", g.topic), attribute.String("outcome", result))
		s.consumed.Add(logCtx, 1, set)
		s.process.Record(logCtx, time.Since(start).Seconds(), set)
	}()

	var err error
	for attempt := 0; ; attempt++ {
		if err = safeHandle(logCtx, g.handle, msg.Key, msg.Value); err == nil {
			if attempt > 0 {
				result = "retried"
			}
			log.Debug("message handled", "topic", msg.Topic, "offset", msg.Offset, "attempts", attempt+1)
			return nil
		}
		if attempt >= s.maxRetries {
			break
		}
		log.Warn("handler failed, retrying", "topic", msg.Topic, "offset", msg.Offset, "attempt", attempt+1, "error", err)
		select {
		case <-ctx.Done():
			result = "interrupted"
			return ctx.Err()
		case <-time.After(s.retryBackoff << attempt):
		}
	}

	log.Error(err, "handler failed, sending to dead letter topic",
		"topic", msg.Topic, "offset", msg.Offset, "dlq", msg.Topic+dlqSuffix)
	dlq := &sarama.ProducerMessage{
		Topic: msg.Topic + dlqSuffix,
		Key:   encoder(msg.Key),
		Value: encoder(msg.Value),
		Headers: []sarama.RecordHeader{
			{Key: []byte("x-error"), Value: []byte(err.Error())},
			{Key: []byte("x-original-topic"), Value: []byte(msg.Topic)},
			{Key: []byte("x-original-partition"), Value: []byte(strconv.Itoa(int(msg.Partition)))},
			{Key: []byte("x-original-offset"), Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		},
	}
	if dlqErr := s.send(logCtx, dlq); dlqErr != nil {
		result = "dlq_failed"
		log.Fatal(dlqErr, "dead letter publish failed; message will be redelivered", "topic", msg.Topic, "offset", msg.Offset)
		return dlqErr
	}
	result = "dead_letter"
	return nil
}

// safeHandle runs h, turning a panic into an error so one bad message cannot crash the consumer.
func safeHandle(ctx context.Context, h Handler, key, value []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h(ctx, key, value)
}

func encoder(b []byte) sarama.Encoder {
	if b == nil {
		return nil
	}
	return sarama.ByteEncoder(b)
}

func outcome(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}
