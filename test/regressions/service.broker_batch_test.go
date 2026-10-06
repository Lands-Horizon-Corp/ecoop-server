package regressions

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broker"
)

// Tests for batching and rate limiting in the Kafka broker. They use the Kafka in docker-compose.yml,
// with a unique topic and group per test.

// collectBatches runs SubscribeBatch in the background and returns every batch the handler saw, plus a
// stop func that ends the subscription and returns what SubscribeBatch returned.
func collectBatches(t *testing.T, b broker.BatchBrokerServices, topic string, handler func([]broker.Message) error) (<-chan []broker.Message, func() error) {
	t.Helper()
	got := make(chan []broker.Message, 256)
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() {
		done <- b.SubscribeBatch(ctx, topic, func(batch []broker.Message) error {
			if handler != nil {
				if err := handler(batch); err != nil {
					return err
				}
			}
			got <- batch
			return nil
		})
	}()
	var once sync.Once
	var result error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(15 * time.Second):
				result = errors.New("SubscribeBatch did not return after its context ended")
			}
		})
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return got, stop
}

func nextBatch(t *testing.T, ch <-chan []broker.Message) []broker.Message {
	t.Helper()
	select {
	case batch := <-ch:
		return batch
	case <-time.After(20 * time.Second):
		t.Fatal("no batch arrived")
		return nil
	}
}

// drain reads batches until n messages have arrived and returns their values, the number of batches and
// the largest batch.
func drain(t *testing.T, ch <-chan []broker.Message, n int) (values []string, batches, largest int) {
	t.Helper()
	for len(values) < n {
		batch := nextBatch(t, ch)
		batches++
		largest = max(largest, len(batch))
		for _, m := range batch {
			values = append(values, string(m.Value))
		}
	}
	return values, batches, largest
}

func messages(n int) []broker.Message {
	msgs := make([]broker.Message, n)
	for i := range msgs {
		msgs[i] = broker.Message{Key: fmt.Appendf(nil, "k%d", i), Value: fmt.Appendf(nil, "v%03d", i)}
	}
	return msgs
}

func TestBrokerBatch_ABatchIsSentAsSoonAsItIsFull(t *testing.T) {
	topic := uniqueName("full")
	opts := broker.Options{
		Producer: broker.ProducerOptions{BatchSize: 5, BatchWait: time.Hour},
		Consumer: broker.ConsumerOptions{BatchSize: 5, BatchWait: time.Hour},
	}
	b := newBrokerWith(t, uniqueName("g"), opts, nil)
	got, _ := collectBatches(t, b, topic, nil)

	for i := range 5 {
		if err := b.Enqueue(bg, topic, nil, fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// BatchWait is an hour, so only the size can have triggered this.
	values, _, _ := drain(t, got, 5)
	if len(values) != 5 {
		t.Fatalf("got %v", values)
	}
}

func TestBrokerBatch_APartialBatchIsSentOnceItsFirstMessageHasWaitedLongEnough(t *testing.T) {
	topic := uniqueName("partial")
	const wait = 500 * time.Millisecond
	opts := broker.Options{
		Producer: broker.ProducerOptions{BatchSize: 100, BatchWait: wait},
		Consumer: broker.ConsumerOptions{BatchSize: 100, BatchWait: wait},
	}
	b := newBrokerWith(t, uniqueName("g"), opts, nil)
	got, _ := collectBatches(t, b, topic, nil)

	started := time.Now()
	for i := range 3 {
		if err := b.Enqueue(bg, topic, nil, fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// 3 of 100 never fill a batch: both sides must give up waiting and deliver what they have.
	values, _, _ := drain(t, got, 3)
	if len(values) != 3 {
		t.Fatalf("got %v", values)
	}
	if elapsed := time.Since(started); elapsed < wait-100*time.Millisecond {
		t.Errorf("delivered after %v; the producer should have held the messages for about %v first", elapsed, wait)
	}
}

func TestBrokerBatch_FlushSendsBufferedMessagesWithoutWaiting(t *testing.T) {
	topic := uniqueName("flush")
	opts := broker.Options{
		Producer: broker.ProducerOptions{BatchSize: 100, BatchWait: time.Hour},
		Consumer: broker.ConsumerOptions{BatchSize: 3, BatchWait: time.Hour},
	}
	b := newBrokerWith(t, uniqueName("g"), opts, nil)
	got, _ := collectBatches(t, b, topic, nil)

	for i := range 3 {
		if err := b.Enqueue(bg, topic, nil, fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Flush(bg); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if values, _, _ := drain(t, got, 3); len(values) != 3 {
		t.Fatalf("got %v", values)
	}
}

func TestBrokerBatch_StopSendsWhatIsStillBuffered(t *testing.T) {
	topic := uniqueName("stop")
	producer := broker.NewBrokerService(kafkaBrokers(t), "g", broker.Options{
		Producer: broker.ProducerOptions{BatchSize: 100, BatchWait: time.Hour},
	}, nil)
	if err := producer.Enqueue(bg, topic, nil, []byte("x")); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("Enqueue before Run = %v; want ErrNotRunning", err)
	}
	if err := producer.Flush(bg); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("Flush before Run = %v; want ErrNotRunning", err)
	}
	if err := producer.Run(bg); err != nil {
		t.Fatal(err)
	}
	for i := range 7 {
		if err := producer.Enqueue(bg, topic, nil, fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// 7 messages, an hour of BatchWait left: only Stop can get them out.
	if err := producer.Stop(bg); err != nil {
		t.Fatal(err)
	}
	if err := producer.Enqueue(bg, topic, nil, []byte("late")); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("Enqueue after Stop = %v; want ErrNotRunning", err)
	}

	reader := newBrokerWith(t, uniqueName("g"), broker.Options{
		Consumer: broker.ConsumerOptions{BatchSize: 7, BatchWait: time.Hour},
	}, nil)
	got, _ := collectBatches(t, reader, topic, nil)
	if values, _, _ := drain(t, got, 7); len(values) != 7 {
		t.Fatalf("got %v; want the 7 buffered messages", values)
	}
}

// With 20 messages per second and a burst of 5, 60 messages cannot go out in under ~2.75s. The producer
// buffer is small, so Enqueue itself must be slowed down (backpressure) rather than buffer without limit.
func TestBrokerBatch_ProducerRateLimitSpreadsMessagesAndSlowsTheCaller(t *testing.T) {
	topic := uniqueName("prate")
	opts := broker.Options{
		Producer: broker.ProducerOptions{BatchSize: 5, BatchWait: 50 * time.Millisecond, MaxPending: 5, Rate: 20, Burst: 5},
		Consumer: broker.ConsumerOptions{BatchSize: 20, BatchWait: 200 * time.Millisecond},
	}
	b := newBrokerWith(t, uniqueName("g"), opts, nil)
	got, _ := collectBatches(t, b, topic, nil)

	const n = 60
	started := time.Now()
	for i := range n {
		ctx, cancel := context.WithTimeout(bg, 10*time.Second)
		err := b.Enqueue(ctx, topic, nil, fmt.Appendf(nil, "v%03d", i))
		cancel()
		if err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	enqueued := time.Since(started)
	if enqueued < time.Second {
		t.Errorf("enqueuing %d messages took %v; a full buffer should have made the caller wait", n, enqueued)
	}
	if err := b.Flush(bg); err != nil {
		t.Fatal(err)
	}
	values, _, _ := drain(t, got, n)
	if elapsed := time.Since(started); elapsed < 2*time.Second {
		t.Errorf("%d messages arrived in %v; at 20/s with a burst of 5 that takes about 2.75s", n, elapsed)
	}
	for i, v := range values {
		if v != fmt.Sprintf("v%03d", i) {
			t.Fatalf("message %d = %q; messages on one partition must stay in order", i, v)
		}
	}
}

func TestBrokerBatch_PublishBatchSendsAllMessagesAndConsumerRateLimitSpacesThemOut(t *testing.T) {
	topic := uniqueName("crate")
	opts := broker.Options{
		Consumer: broker.ConsumerOptions{BatchSize: 5, BatchWait: 100 * time.Millisecond, Rate: 20, Burst: 5},
	}
	b := newBrokerWith(t, uniqueName("g"), opts, nil)

	const n = 30
	if err := b.PublishBatch(bg, topic, messages(n)); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	got, _ := collectBatches(t, b, topic, nil)

	first := nextBatch(t, got)
	firstAt := time.Now()
	rest, _, largest := drain(t, got, n-len(first))
	if len(first)+len(rest) != n {
		t.Fatalf("received %d messages; want %d", len(first)+len(rest), n)
	}
	largest = max(largest, len(first))
	if largest > 5 {
		t.Errorf("a batch held %d messages; the burst limit is 5", largest)
	}
	// 25 messages beyond the first batch at 20/s take 1.25s.
	if elapsed := time.Since(firstAt); elapsed < time.Second {
		t.Errorf("the remaining %d messages were handled in %v; the consumer rate limit should spread them over ~1.25s", n-len(first), elapsed)
	}
}

func TestBrokerBatch_AFailedBatchIsRedeliveredWholeToTheNextSubscriber(t *testing.T) {
	topic := uniqueName("bredeliver")
	opts := broker.Options{Consumer: broker.ConsumerOptions{BatchSize: 4, BatchWait: time.Hour}}
	b := newBrokerWith(t, uniqueName("g"), opts, nil)
	if err := b.PublishBatch(bg, topic, messages(4)); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("database is down")
	// The handler fails on the first batch, which ends SubscribeBatch with that error.
	failedCh := make(chan error, 1)
	go func() {
		failedCh <- b.SubscribeBatch(bg, topic, func([]broker.Message) error { return boom })
	}()
	select {
	case err := <-failedCh:
		if !errors.Is(err, boom) {
			t.Fatalf("SubscribeBatch returned %v; want the handler's error", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("SubscribeBatch did not stop after its handler failed")
	}

	second, _ := collectBatches(t, b, topic, nil)
	values, _, _ := drain(t, second, 4)
	for i, v := range values {
		if v != fmt.Sprintf("v%03d", i) {
			t.Fatalf("redelivered %v; want all four messages again, in order", values)
		}
	}
}

func TestBrokerBatch_UndeliverableBatchesAreReportedToOnErrorAndTheLog(t *testing.T) {
	failed := make(chan string, 4)
	var mu sync.Mutex
	var gotMsgs []broker.Message
	log := &recordingLog{Context: bg}
	opts := broker.Options{Producer: broker.ProducerOptions{
		BatchSize: 100,
		BatchWait: time.Hour,
		OnError: func(topic string, msgs []broker.Message, err error) {
			mu.Lock()
			gotMsgs = append(gotMsgs, msgs...)
			mu.Unlock()
			failed <- topic
		},
	}}
	b := newBrokerWith(t, uniqueName("g"), opts, log)

	const badTopic = "not a valid topic!"
	if err := b.Enqueue(bg, badTopic, []byte("k"), []byte("lost-without-onerror")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(bg, 30*time.Second)
	defer cancel()
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case topic := <-failed:
		if topic != badTopic {
			t.Errorf("OnError topic = %q; want %q", topic, badTopic)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("OnError was never called for an undeliverable batch")
	}
	mu.Lock()
	if len(gotMsgs) != 1 || string(gotMsgs[0].Value) != "lost-without-onerror" {
		t.Errorf("OnError got %+v; want the undelivered message so the caller can retry it", gotMsgs)
	}
	mu.Unlock()

	e := log.await(t, "the failed batch", withMsg("broker batch failed"))
	if e.level != "error" || e.err == nil || e.fields["count"] != 1 || e.fields["reason"] != "flush" {
		t.Errorf("line = %+v; want an error with the count and what triggered the send", e)
	}
}

func TestBrokerBatch_DefaultsAndSettingsAreLogged(t *testing.T) {
	log := &recordingLog{Context: bg}
	newBroker(t, uniqueName("g"), log)
	e := log.await(t, "readiness", withMsg("broker ready"))
	for key, want := range map[string]any{
		"producer_batch_size": 100, "producer_batch_wait": "5s", "producer_max_pending": 1000,
		"consumer_batch_size": 100, "consumer_batch_wait": "5s",
	} {
		if fmt.Sprint(e.fields[key]) != fmt.Sprint(want) {
			t.Errorf("%s = %v; want the default %v", key, e.fields[key], want)
		}
	}

	log = &recordingLog{Context: bg}
	newBrokerWith(t, uniqueName("g"), broker.Options{
		Producer: broker.ProducerOptions{BatchSize: 20, Rate: 40},
		Consumer: broker.ConsumerOptions{BatchSize: 50, Rate: 10, Burst: 8},
	}, log)
	e = log.await(t, "readiness", withMsg("broker ready"))
	for key, want := range map[string]any{
		"producer_batch_size": 20, "producer_max_pending": 200, "producer_rate": 40,
		// Consumer batches never exceed the burst, or a full batch could never get its tokens.
		"consumer_batch_size": 8, "consumer_rate": 10,
	} {
		if fmt.Sprint(e.fields[key]) != fmt.Sprint(want) {
			t.Errorf("%s = %v; want %v", key, e.fields[key], want)
		}
	}
}
