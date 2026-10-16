package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// commitTimeout bounds one offset commit. The commit deliberately outlives
// the run context: a batch that was applied must be acknowledged even while
// the service is stopping, or it would only be delivered again.
const commitTimeout = 10 * time.Second

// SubscribeBatch implements ICDCService.
func (c *CDCService) SubscribeBatch(ctx context.Context, topic string, opts BatchOptions, handler BatchHandler) error {
	if c == nil || c.subs == nil {
		return ErrNoService
	}
	if ctx == nil {
		return ErrNilContext
	}
	var errs []error
	if strings.TrimSpace(topic) == "" {
		errs = append(errs, ErrNoTopic)
	}
	if handler == nil {
		errs = append(errs, ErrNoBatchHandler)
	}
	if c.groupID == "" {
		errs = append(errs, ErrNoGroup)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = DefaultMaxBatch
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.subs[topic]; ok {
		return ErrDuplicateSubscription
	}
	s := &subscription{topic: topic, batch: handler, opts: opts}
	if c.producer != nil {
		if err := c.consume(s); err != nil {
			return err
		}
	}
	c.subs[topic] = s
	return nil
}

// GroupID implements ICDCService.
func (c *CDCService) GroupID() string {
	if c == nil {
		return ""
	}
	return c.groupID
}

// DeadLetter implements ICDCService.
func (c *CDCService) DeadLetter(ctx context.Context, rec Record, cause error) error {
	if c == nil || c.subs == nil {
		return ErrNoService
	}
	if ctx == nil {
		return ErrNilContext
	}
	if cause == nil {
		cause = errors.New("unspecified")
	}
	c.mu.Lock()
	producer := c.producer
	c.mu.Unlock()
	if producer == nil {
		return ErrNotRunning
	}
	headers := make([]kgo.RecordHeader, len(rec.Headers))
	for i, h := range rec.Headers {
		headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
	}
	dead := c.dlqRecord(rec.Topic, rec.Partition, rec.Offset, []byte(rec.Key), rec.Value, headers, cause, 1)
	if err := producer.ProduceSync(ctx, dead).FirstErr(); err != nil {
		return fmt.Errorf("cdc: dead-lettering %s[%d]@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
	}
	return nil
}

// dlqRecord is the dead-letter copy of a record: its key, value and headers,
// plus where it came from and why it was parked.
func (c *CDCService) dlqRecord(
	topic string, partition int32, offset int64, key, value []byte, headers []kgo.RecordHeader, cause error, attempts int,
) *kgo.Record {
	return &kgo.Record{
		Topic: topic + c.dlqSuffix,
		Key:   key,
		Value: value,
		Headers: append(append([]kgo.RecordHeader(nil), headers...),
			kgo.RecordHeader{Key: HeaderDLQTopic, Value: []byte(topic)},
			kgo.RecordHeader{Key: HeaderDLQPartition, Value: []byte(strconv.Itoa(int(partition)))},
			kgo.RecordHeader{Key: HeaderDLQOffset, Value: []byte(strconv.FormatInt(offset, 10))},
			kgo.RecordHeader{Key: HeaderDLQError, Value: []byte(cause.Error())},
			kgo.RecordHeader{Key: HeaderDLQAttempts, Value: []byte(strconv.Itoa(attempts))},
			kgo.RecordHeader{Key: HeaderDLQGroup, Value: []byte(c.group(topic))},
			kgo.RecordHeader{Key: HeaderDLQFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		),
	}
}

// consumeBatch starts the consumer of a batch subscription. The caller holds c.mu.
func (c *CDCService) consumeBatch(s *subscription) error {
	ctx := c.runCtx
	if ctx == nil {
		return ErrNotRunning
	}
	kopts := []kgo.Opt{
		kgo.SeedBrokers(c.brokers...),
		kgo.ConsumerGroup(c.group(s.topic)),
		kgo.ConsumeTopics(s.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// No AllowAutoTopicCreation: the topic belongs to Debezium, which creates it
		// with the partition count it is configured for. A consumer that created it
		// first would get the broker's default (usually 1) and the stream could never
		// be shared between replicas. Until Debezium writes, the consumer just waits.
		// A rebalance waits until the batch in hand is handled and committed,
		// so this instance never commits partitions it no longer owns.
		kgo.BlockRebalanceOnPoll(),
	}
	if s.opts.OnAssigned != nil {
		on := s.opts.OnAssigned
		kopts = append(kopts, kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			on(m[s.topic])
		}))
	}
	if s.opts.OnRevoked != nil {
		on := s.opts.OnRevoked
		revoked := func(_ context.Context, _ *kgo.Client, m map[string][]int32) { on(m[s.topic]) }
		kopts = append(kopts, kgo.OnPartitionsRevoked(revoked), kgo.OnPartitionsLost(revoked))
	}
	client, err := kgo.NewClient(kopts...)
	if err != nil {
		return errors.Join(ErrConnect, err)
	}
	sctx, cancel := context.WithCancel(ctx)
	s.client, s.cancel, s.done = client, cancel, make(chan struct{})
	c.loops.Go(func() {
		defer close(s.done)
		defer cancel()
		c.batchLoop(sctx, client, s)
	})
	return nil
}

// Unsubscribe implements ICDCService. The handler running now is told to stop
// (its context ends); whatever it had not finished is delivered again to the
// next consumer of the group.
func (c *CDCService) Unsubscribe(ctx context.Context, topic string) error {
	if c == nil || c.subs == nil {
		return ErrNoService
	}
	if ctx == nil {
		return ErrNilContext
	}
	c.mu.Lock()
	s, ok := c.subs[topic]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	delete(c.subs, topic)
	client, cancel, done := s.client, s.cancel, s.done
	s.client = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	var err error
	if done != nil {
		// Let the loop finish the commit of a batch it already applied before
		// the connection goes away.
		select {
		case <-done:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	if client != nil {
		client.CloseAllowingRebalance()
	}
	return err
}

// batchLoop polls the topic and hands each partition's records, in order, to
// the handler. It ends when the service stops; nothing else ends it, so a
// failed commit or a failing handler cannot silently stop the topic.
func (c *CDCService) batchLoop(ctx context.Context, client *kgo.Client, s *subscription) {
	for {
		fetches := client.PollRecords(ctx, s.opts.MaxBatch)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		keepGoing := true
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for chunk := range utils.Chunks(p.Records, s.opts.MaxBatch) {
				if !keepGoing {
					return
				}
				keepGoing = c.handleChunk(ctx, client, s, chunk)
			}
		})
		if !keepGoing {
			return
		}
		client.AllowRebalance()
	}
}

// handleChunk delivers one run of consecutive records of a partition and
// commits it. It returns false when the service is stopping.
func (c *CDCService) handleChunk(ctx context.Context, client *kgo.Client, s *subscription, records []*kgo.Record) bool {
	done, ok := c.gate.Enter()
	if !ok {
		return false
	}
	defer done()
	batch := make([]Record, len(records))
	for i, r := range records {
		batch[i] = toRecord(r)
	}
	if !c.deliver(ctx, s.batch, batch) {
		return false // stopping: nothing is committed, so it is delivered again
	}
	last := records[len(records)-1]
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()
	if err := client.CommitRecords(cctx, last); err != nil && s.opts.OnCommitError != nil {
		s.opts.OnCommitError(last.Partition, last.Offset, err)
	}
	return true
}

// deliver calls handler until it returns nil, backing off between attempts,
// and returns false if the service is stopping first.
func (c *CDCService) deliver(ctx context.Context, handler BatchHandler, batch []Record) bool {
	wait := c.backoff
	for {
		if safeBatch(ctx, handler, batch) == nil {
			return true
		}
		if !c.gate.IsOpen() || ctx.Err() != nil {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(wait):
		}
		wait = min(wait*2, maxDLQBackoff)
	}
}

// safeBatch runs handler and turns a panic into an error, so a bug in one
// batch is retried and reported instead of taking the process down.
func safeBatch(ctx context.Context, handler BatchHandler, batch []Record) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("cdc: batch handler panicked: %v", p)
		}
	}()
	return handler(ctx, batch)
}

// toRecord copies a Kafka record into a Record and decodes its value.
func toRecord(r *kgo.Record) Record {
	rec := Record{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       json.RawMessage(r.Key),
		Value:     r.Value,
		Timestamp: r.Timestamp,
	}
	if len(r.Headers) > 0 {
		rec.Headers = make([]Header, len(r.Headers))
		for i, h := range r.Headers {
			rec.Headers[i] = Header{Key: h.Key, Value: h.Value}
		}
	}
	rec.Event, rec.Err = Decode(r.Value)
	return rec
}

// Decode decodes the value of a Debezium record, with or without the schema
// wrapper the JSON converter can add. An empty value is a tombstone: no event
// and no error. Anything else must be a change event with an operation, or the
// error wraps ErrNotChangeEvent.
func Decode(value []byte) (*ChangeEvent, error) {
	if len(value) == 0 {
		return nil, nil
	}
	event, err := utils.Decode[ChangeEvent](value)
	if err != nil {
		return nil, errors.Join(ErrNotChangeEvent, err)
	}
	if event == nil || event.Op == "" {
		return nil, ErrNotChangeEvent
	}
	return event, nil
}
