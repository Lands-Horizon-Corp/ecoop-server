package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/twmb/franz-go/pkg/kgo"
)

// errStopped ends SubscribeBatch quietly when its context ends mid-batch.
var errStopped = errors.New("broker: subscription stopped")

// slowEnqueue is how long Enqueue may block on a full buffer before it is worth a warning.
const slowEnqueue = time.Second

// startLoop starts the goroutine behind Enqueue. The caller holds m.mu and has set m.producer.
func (m *MessageBrokerService) startLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	m.queue = make(chan *kgo.Record, m.opts.Producer.MaxPending)
	m.stopCh = make(chan struct{})
	m.flushReq = make(chan chan struct{})
	m.loopDone = make(chan struct{})
	m.cancel = cancel
	go m.runLoop(ctx, m.producer, m.queue, m.flushReq, m.loopDone)
}

// runLoop collects queued records into a batch and sends it when it is full, when its first record has
// waited BatchWait, on Flush, and when the queue is closed by Stop. It sends batches one at a time, so a
// slow broker or the rate limit makes the queue fill and Enqueue block: that is the backpressure.
func (m *MessageBrokerService) runLoop(ctx context.Context, producer *kgo.Client, in <-chan *kgo.Record, flushReq <-chan chan struct{}, done chan<- struct{}) {
	defer close(done)
	size, wait := m.opts.Producer.BatchSize, m.opts.Producer.BatchWait

	var (
		batch   []*kgo.Record
		timer   *time.Timer
		timeout <-chan time.Time
	)
	send := func(reason string) {
		if timer != nil {
			timer.Stop()
			timer, timeout = nil, nil
		}
		if len(batch) == 0 {
			return
		}
		m.sendBatch(ctx, producer, batch, reason, len(in))
		batch = nil
	}
	add := func(rec *kgo.Record) {
		batch = append(batch, rec)
		if len(batch) == 1 {
			timer = time.NewTimer(wait)
			timeout = timer.C
		}
		if len(batch) >= size {
			send("size")
		}
	}

	for {
		select {
		case rec, ok := <-in:
			if !ok {
				send("stop")
				return
			}
			add(rec)
		case <-timeout:
			send("time")
		case ack := <-flushReq:
			for drained := false; !drained; {
				select {
				case rec, ok := <-in:
					if !ok {
						drained = true
						break
					}
					add(rec)
				default:
					drained = true
				}
			}
			send("flush")
			close(ack)
		}
	}
}

// produce sends recs in chunks no larger than the rate limiter's burst, waiting for tokens before each.
// It returns the records that were not delivered and the first error.
func (m *MessageBrokerService) produce(ctx context.Context, producer *kgo.Client, recs []*kgo.Record) ([]*kgo.Record, error) {
	var (
		failed   []*kgo.Record
		firstErr error
	)
	for len(recs) > 0 {
		n := len(recs)
		if m.publishLimiter != nil {
			n = min(n, m.opts.Producer.Burst)
			if err := m.waitPublish(ctx, n); err != nil {
				return append(failed, recs...), errors.Join(firstErr, err)
			}
		}
		for _, res := range producer.ProduceSync(ctx, recs[:n]...) {
			if res.Err != nil {
				failed = append(failed, res.Record)
				if firstErr == nil {
					firstErr = res.Err
				}
			}
		}
		recs = recs[n:]
	}
	return failed, firstErr
}

// sendBatch delivers a batch built by the loop and reports the outcome.
func (m *MessageBrokerService) sendBatch(ctx context.Context, producer *kgo.Client, batch []*kgo.Record, reason string, queued int) {
	started := time.Now()
	failed, err := m.produce(ctx, producer, batch)
	elapsed := time.Since(started).Milliseconds()

	if err != nil {
		err = fmt.Errorf("sending a batch of %d messages (%d failed): %w", len(batch), len(failed), err)
		m.emit("broker.batch", func(l logger.LoggerLevel) {
			l.Error(err, "broker batch failed", "component", "broker", "count", len(batch),
				"failed", len(failed), "reason", reason, "pending", queued, "duration_ms", elapsed)
		})
		m.reportFailed(failed, err)
		return
	}
	m.emit("broker.batch", func(l logger.LoggerLevel) {
		l.Debug("broker batch published", "component", "broker", "count", len(batch),
			"reason", reason, "pending", queued, "duration_ms", elapsed)
	})
}

// reportFailed hands undelivered records to OnError, grouped by topic.
func (m *MessageBrokerService) reportFailed(failed []*kgo.Record, err error) {
	onError := m.opts.Producer.OnError
	if onError == nil {
		return
	}
	byTopic := map[string][]Message{}
	for _, rec := range failed {
		byTopic[rec.Topic] = append(byTopic[rec.Topic], Message{Key: rec.Key, Value: rec.Value, Topic: rec.Topic})
	}
	for topic, msgs := range byTopic {
		onError(topic, msgs, err)
	}
}

// PublishBatch sends msgs to topic in one request (or several when the burst limit is smaller than
// len(msgs)) and returns once the brokers acknowledged them.
func (m *MessageBrokerService) PublishBatch(ctx context.Context, topic string, msgs []Message) error {
	m.mu.RLock()
	producer := m.producer
	m.mu.RUnlock()
	if producer == nil {
		return ErrNotRunning
	}
	if len(msgs) == 0 {
		return nil
	}
	return m.observe("broker.publish_batch", topic, func() error {
		recs := make([]*kgo.Record, len(msgs))
		for i, msg := range msgs {
			recs[i] = &kgo.Record{Topic: topic, Key: msg.Key, Value: msg.Value}
		}
		if failed, err := m.produce(ctx, producer, recs); err != nil {
			return fmt.Errorf("publishing %d of %d messages to %s: %w", len(failed), len(msgs), topic, err)
		}
		return nil
	}, "count", len(msgs))
}

// Enqueue adds a message to the buffer that the loop sends in batches.
func (m *MessageBrokerService) Enqueue(ctx context.Context, topic string, key, value []byte) error {
	m.mu.RLock()
	queue, stop := m.queue, m.stopCh
	if queue == nil {
		m.mu.RUnlock()
		return ErrNotRunning
	}
	m.inflight.Add(1)
	m.mu.RUnlock()
	defer m.inflight.Done()

	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	select {
	case queue <- rec:
		return nil
	default:
	}

	started := time.Now()
	select {
	case queue <- rec:
	case <-stop:
		return ErrNotRunning
	case <-ctx.Done():
		return ctx.Err()
	}
	if waited := time.Since(started); waited >= slowEnqueue {
		m.emit("broker.backpressure", func(l logger.LoggerLevel) {
			l.Warn("broker buffer is full, the producer was slowed", "component", "broker", "topic", topic,
				"waited_ms", waited.Milliseconds(), "max_pending", m.opts.Producer.MaxPending)
		})
	}
	return nil
}

// Flush sends what Enqueue has buffered so far and waits until that batch has been attempted.
func (m *MessageBrokerService) Flush(ctx context.Context) error {
	m.mu.RLock()
	req, loopDone := m.flushReq, m.loopDone
	m.mu.RUnlock()
	if req == nil {
		return ErrNotRunning
	}
	ack := make(chan struct{})
	select {
	case req <- ack:
	case <-loopDone:
		return nil // Stop is already sending the rest
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SubscribeBatch delivers a topic's records to handler in batches until ctx ends (it then returns nil).
// A partial batch that is still waiting when ctx ends is not committed and is delivered again later.
func (m *MessageBrokerService) SubscribeBatch(ctx context.Context, topic string, handler func(batch []Message) error) error {
	client, err := m.consumer(topic)
	if err != nil {
		return err
	}
	defer client.Close()
	size, wait := m.opts.Consumer.BatchSize, m.opts.Consumer.BatchWait

	var (
		batch   []*kgo.Record
		started time.Time
	)
	for {
		// A partial batch only waits until its first record is BatchWait old.
		pollCtx, cancel := ctx, context.CancelFunc(func() {})
		if len(batch) > 0 {
			pollCtx, cancel = context.WithDeadline(ctx, started.Add(wait))
		}
		fetches := client.PollRecords(pollCtx, size-len(batch))
		cancel()
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		m.logFetchErrors(fetches)

		if recs := fetches.Records(); len(recs) > 0 {
			if len(batch) == 0 {
				started = time.Now()
			}
			batch = append(batch, recs...)
		}

		reason := ""
		switch {
		case len(batch) >= size:
			reason = "size"
		case len(batch) > 0 && !time.Now().Before(started.Add(wait)):
			reason = "time"
		default:
			continue
		}
		if err := m.handleBatch(ctx, client, topic, batch, reason, handler); err != nil {
			if errors.Is(err, errStopped) {
				return nil
			}
			return err
		}
		batch = nil
	}
}

func (m *MessageBrokerService) handleBatch(ctx context.Context, client *kgo.Client, topic string, batch []*kgo.Record, reason string, handler func([]Message) error) error {
	if m.consumeLimiter != nil {
		if err := m.consumeLimiter.WaitN(ctx, len(batch)); err != nil {
			return errStopped
		}
	}
	msgs := make([]Message, len(batch))
	for i, rec := range batch {
		msgs[i] = Message{Key: rec.Key, Value: rec.Value, Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
	}

	started := time.Now()
	if err := handler(msgs); err != nil {
		first := batch[0]
		err = fmt.Errorf("batch handler failed on %d messages starting at %s[%d]@%d: %w",
			len(batch), first.Topic, first.Partition, first.Offset, err)
		m.emit("broker.handler", func(l logger.LoggerLevel) {
			l.Error(err, "broker batch handler failed", "component", "broker", "topic", topic,
				"count", len(batch), "partition", first.Partition, "offset", first.Offset)
		})
		return err
	}
	m.commit(ctx, client, topic, batch)
	m.emit("broker.handler", func(l logger.LoggerLevel) {
		l.Debug("broker batch handled", "component", "broker", "topic", topic, "count", len(batch),
			"reason", reason, "duration_ms", time.Since(started).Milliseconds())
	})
	return nil
}
