package cdc

import (
	"context"
	"encoding/json"
	"errors"
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

	// runCtx lives from Run to Stop (it keeps Run's values but not its
	// cancellation, so a short startup context cannot kill the consumers).
	// loops tracks every consumer goroutine so Stop leaves none behind.
	runCtx    context.Context
	cancelRun context.CancelFunc
	loops     sync.WaitGroup
}

type subscription struct {
	topic   string
	handler Handler
	batch   BatchHandler
	opts    BatchOptions
	client  *kgo.Client

	// Batch subscriptions only: cancel ends the consumer loop, done closes
	// when it has ended.
	cancel context.CancelFunc
	done   chan struct{}
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
	c.runCtx, c.cancelRun = context.WithCancel(context.WithoutCancel(ctx))
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
	// No new work from here on; what is running gets until ctx ends to finish.
	// A batch handler that keeps failing stops retrying as soon as the gate is
	// closed, so this cannot wait on it forever.
	err := c.gate.Close(ctx)

	// Order matters. A batch consumer that polls again after the clients start
	// closing blocks rebalances once more, and leaving the group then waits on a
	// rebalance that can never happen. So first end the run context (the loops
	// stop polling), wait for the batch loops, and only then close the clients.
	c.mu.Lock()
	if c.cancelRun != nil {
		c.cancelRun()
	}
	var batchDone []chan struct{}
	for _, s := range c.subs {
		if s.done != nil {
			batchDone = append(batchDone, s.done)
		}
	}
	c.mu.Unlock()
	for _, d := range batchDone {
		select {
		case <-d:
		case <-ctx.Done():
			if err == nil {
				err = ctx.Err()
			}
		}
	}

	c.mu.Lock()
	c.closeLocked() // also aborts whatever outlived ctx
	c.mu.Unlock()
	if werr := waitLoops(ctx, &c.loops); err == nil {
		err = werr
	}
	return err
}

// waitLoops waits for every consumer goroutine, or returns ctx.Err().
func waitLoops(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *CDCService) closeLocked() {
	if c.cancelRun != nil {
		c.cancelRun()
		c.cancelRun, c.runCtx = nil, nil
	}
	for _, s := range c.subs {
		if s.client != nil {
			// Close alone hangs on a client that polled and has rebalances blocked.
			s.client.CloseAllowingRebalance()
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
	if s.batch != nil {
		return c.consumeBatch(s)
	}
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
	c.loops.Go(func() { c.loop(client, s.handler) })
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
			// A failed commit (a rebalance, a broker blip) only means the record is
			// delivered again later, which the contract allows. It must not end
			// the loop: that would silently stop this topic for good.
			_ = client.CommitRecords(context.Background(), r)
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
	dead := c.dlqRecord(r.Topic, r.Partition, r.Offset, r.Key, r.Value, r.Headers, cause, c.attempts)
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
