//go:build integration

package cdc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// Delivery against a real Kafka: what a handler is promised about order, batching,
// position, retries, commits and shutdown.

// perPartitionOrder fails the test if any partition's offsets are not strictly increasing.
func perPartitionOrder(t testing.TB, recs []cdc.Record) {
	t.Helper()
	last := map[int32]int64{}
	for _, r := range recs {
		if prev, ok := last[r.Partition]; ok && r.Offset <= prev {
			t.Fatalf("partition %d went from offset %d to %d", r.Partition, prev, r.Offset)
		}
		last[r.Partition] = r.Offset
	}
}

func publishN(t testing.TB, topic string, n int) {
	t.Helper()
	p := producer(t)
	for i := range n {
		if err := p.Publish(ctxT(t), topic, key(i), event(cdc.OpCreate, i, "x")); err != nil {
			t.Fatal(err)
		}
	}
}

// ---- batching and position -------------------------------------------------

func TestDelivery_MaxBatchIsRespected(t *testing.T) {
	for _, max := range []int{1, 2, 5, 17} {
		t.Run(fmt.Sprintf("max=%d", max), func(t *testing.T) {
			t.Parallel()
			topic := names()
			createTopic(t, topic, 1)
			publishN(t, topic, 40)
			s := service(t)
			var sink batchSink
			if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{MaxBatch: max}, sink.handle); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "40 records", func() bool { return len(sink.records()) == 40 })
			sink.mu.Lock()
			defer sink.mu.Unlock()
			for i, b := range sink.batches {
				if len(b) > max {
					t.Fatalf("batch %d holds %d records, max is %d", i, len(b), max)
				}
			}
		})
	}
}

func TestDelivery_BatchesAreConsecutiveRunsOfOnePartition(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 3)
	publishN(t, topic, 90)
	s := service(t)
	var sink batchSink
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{MaxBatch: 7}, sink.handle); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "90 records", func() bool { return len(sink.records()) == 90 })
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for i, b := range sink.batches {
		for j := 1; j < len(b); j++ {
			if b[j].Partition != b[0].Partition || b[j].Offset != b[j-1].Offset+1 {
				t.Fatalf("batch %d is not a consecutive run of one partition at index %d: %+v then %+v", i, j, b[j-1], b[j])
			}
		}
	}
}

func TestDelivery_DefaultMaxBatchApplies(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 30)
	s := service(t)
	var sink batchSink
	// The zero value of BatchOptions is usable.
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "30 records", func() bool { return len(sink.records()) == 30 })
}

func TestDelivery_NegativeMaxBatchFallsBackToTheDefault(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 10)
	s := service(t)
	var sink batchSink
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{MaxBatch: -5}, sink.handle); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "10 records", func() bool { return len(sink.records()) == 10 })
}

func TestDelivery_OffsetsAreContiguousFromZero(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 60)
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{MaxBatch: 9}, sink.handle)
	waitFor(t, "60 records", func() bool { return len(sink.records()) == 60 })
	for i, r := range sink.records() {
		if r.Offset != int64(i) {
			t.Fatalf("record %d has offset %d", i, r.Offset)
		}
	}
}

func TestDelivery_TimestampAndKeyAndValueAreCarried(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	if err := producer(t).Publish(ctxT(t), topic, key(42), event(cdc.OpCreate, 42, "carried")); err != nil {
		t.Fatal(err)
	}
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle)
	waitFor(t, "the record", func() bool { return len(sink.records()) == 1 })
	r := sink.records()[0]
	if string(r.Key) != `{"id":42}` || !strings.Contains(string(r.Value), "carried") || r.Timestamp.IsZero() {
		t.Fatalf("record = key %s value %s ts %v", r.Key, r.Value, r.Timestamp)
	}
	if r.Event == nil || r.Event.Op != cdc.OpCreate {
		t.Fatalf("event = %+v", r.Event)
	}
}

// ---- order -----------------------------------------------------------------

func TestDelivery_OnePartitionOrderIsPreservedForManyRows(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	p := producer(t)
	for i := range 100 {
		_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpUpdate, 1, fmt.Sprintf("v%03d", i)))
	}
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{MaxBatch: 13}, sink.handle)
	waitFor(t, "100 records", func() bool { return len(sink.records()) == 100 })
	for i, r := range sink.records() {
		if !strings.Contains(string(r.Value), fmt.Sprintf("v%03d", i)) {
			t.Fatalf("record %d is out of order: %s", i, r.Value)
		}
	}
}

func TestDelivery_SameKeyStaysOnOnePartition(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 4)
	p := producer(t)
	for round := range 10 {
		for id := range 8 {
			_ = p.Publish(ctxT(t), topic, key(id), event(cdc.OpUpdate, id, fmt.Sprintf("r%d", round)))
		}
	}
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle)
	waitFor(t, "80 records", func() bool { return len(sink.records()) == 80 })
	home := map[string]int32{}
	for _, r := range sink.records() {
		if p, ok := home[string(r.Key)]; ok && p != r.Partition {
			t.Fatalf("key %s moved from partition %d to %d: per-row order would be lost", r.Key, p, r.Partition)
		}
		home[string(r.Key)] = r.Partition
	}
}

func TestDelivery_PerPartitionOffsetsNeverGoBack(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 4)
	publishN(t, topic, 200)
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{MaxBatch: 11}, sink.handle)
	waitFor(t, "200 records", func() bool { return len(sink.records()) == 200 })
	perPartitionOrder(t, sink.records())
}

func TestDelivery_EveryRecordExactlyOnceWithoutFailures(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 3)
	publishN(t, topic, 150)
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{MaxBatch: 20}, sink.handle)
	waitFor(t, "150 records", func() bool { return len(sink.records()) >= 150 })
	time.Sleep(500 * time.Millisecond) // a duplicate would arrive now
	seen := map[string]int{}
	for _, r := range sink.records() {
		seen[fmt.Sprintf("%d/%d", r.Partition, r.Offset)]++
	}
	if len(seen) != 150 {
		t.Fatalf("%d distinct positions, want 150", len(seen))
	}
	for pos, n := range seen {
		if n != 1 {
			t.Fatalf("position %s delivered %d times without any failure", pos, n)
		}
	}
}

// ---- kinds of record -------------------------------------------------------

func TestDelivery_MixedKindsKeepTheirOrder(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	p := producer(t)
	_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "a"))
	_ = p.Publish(ctxT(t), topic, key(1), nil) // tombstone
	_ = p.Publish(ctxT(t), topic, key(2), []byte("garbage"))
	_ = p.Publish(ctxT(t), topic, key(3), event(cdc.OpDelete, 3, "c"))
	_ = p.Publish(ctxT(t), topic, key(4), []byte(`{"payload":{"x":1}}`)) // not a change
	_ = p.Publish(ctxT(t), topic, key(5), event(cdc.OpUpdate, 5, "e"))
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle)
	waitFor(t, "6 records", func() bool { return len(sink.records()) == 6 })
	kinds := make([]string, 0, 6)
	for _, r := range sink.records() {
		switch {
		case r.Err != nil:
			kinds = append(kinds, "poison")
		case r.Event == nil:
			kinds = append(kinds, "tombstone")
		default:
			kinds = append(kinds, string(r.Event.Op))
		}
	}
	if got := strings.Join(kinds, ","); got != "c,tombstone,poison,d,poison,u" {
		t.Fatalf("kinds = %s", got)
	}
}

func TestDelivery_TombstoneHasAKeyButNoValue(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	_ = producer(t).Publish(ctxT(t), topic, key(9), nil)
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle)
	waitFor(t, "the tombstone", func() bool { return len(sink.records()) == 1 })
	r := sink.records()[0]
	if string(r.Key) != `{"id":9}` || len(r.Value) != 0 || r.Event != nil || r.Err != nil {
		t.Fatalf("tombstone = %+v", r)
	}
}

func TestDelivery_PoisonKeepsItsRawValue(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	_ = producer(t).Publish(ctxT(t), topic, key(1), []byte("\x00\x01 not json \xff"))
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle)
	waitFor(t, "the poison record", func() bool { return len(sink.records()) == 1 })
	r := sink.records()[0]
	if !errors.Is(r.Err, cdc.ErrNotChangeEvent) || string(r.Value) != "\x00\x01 not json \xff" {
		t.Fatalf("poison = err %v value %q", r.Err, r.Value)
	}
}

func TestDelivery_PoisonDoesNotStopTheRecordsBehindIt(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	p := producer(t)
	_ = p.Publish(ctxT(t), topic, key(1), []byte("poison one"))
	_ = p.Publish(ctxT(t), topic, key(2), []byte("poison two"))
	for i := 10; i < 15; i++ {
		_ = p.Publish(ctxT(t), topic, key(i), event(cdc.OpCreate, i, "after"))
	}
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle)
	waitFor(t, "7 records", func() bool { return len(sink.records()) == 7 })
	changes := 0
	for _, r := range sink.records() {
		if r.Event != nil {
			changes++
		}
	}
	if changes != 5 {
		t.Fatalf("%d changes behind the poison, want 5", changes)
	}
}

func TestDelivery_SchemaWrappedEventsAreDecoded(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	wrapped := `{"schema":{"type":"struct"},"payload":{"op":"c","before":null,"after":{"id":3,"name":"w"},"ts_ms":1}}`
	_ = producer(t).Publish(ctxT(t), topic, key(3), []byte(wrapped))
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle)
	waitFor(t, "the record", func() bool { return len(sink.records()) == 1 })
	if r := sink.records()[0]; r.Err != nil || r.Event == nil || r.Event.Op != cdc.OpCreate {
		t.Fatalf("record = %+v", r)
	}
}

func TestDelivery_LargeValueArrivesIntact(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	big := strings.Repeat("é", 300_000) // about 600 KB
	_ = producer(t).Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, big))
	s := service(t)
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle)
	waitFor(t, "the large record", func() bool { return len(sink.records()) == 1 })
	r := sink.records()[0]
	if r.Err != nil || r.Event == nil || !strings.Contains(string(r.Event.After), big) {
		t.Fatalf("a 600 KB value must arrive whole: err=%v", r.Err)
	}
}

// ---- retries and commits ---------------------------------------------------

func TestDelivery_RetriedBatchIsTheSameBatch(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 6)
	s := service(t)
	var mu sync.Mutex
	var attempts [][]int64
	var calls atomic.Int64
	h := func(_ context.Context, b []cdc.Record) error {
		offs := make([]int64, len(b))
		for i, r := range b {
			offs[i] = r.Offset
		}
		mu.Lock()
		attempts = append(attempts, offs)
		mu.Unlock()
		if calls.Add(1) <= 2 {
			return errors.New("not yet")
		}
		return nil
	}
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, h)
	waitFor(t, "3 attempts", func() bool { return calls.Load() >= 3 })
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < 3; i++ {
		if fmt.Sprint(attempts[i]) != fmt.Sprint(attempts[0]) {
			t.Fatalf("attempt %d saw %v, the first saw %v: a retry must be the same batch", i, attempts[i], attempts[0])
		}
	}
}

func TestDelivery_RetryBlocksTheLaterRecordsOfThePartition(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 4)
	s := service(t)
	var released atomic.Bool
	var sink batchSink
	h := func(ctx context.Context, b []cdc.Record) error {
		if !released.Load() {
			return errors.New("blocked")
		}
		return sink.handle(ctx, b)
	}
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{MaxBatch: 1}, h)
	time.Sleep(800 * time.Millisecond)
	if n := len(sink.records()); n != 0 {
		t.Fatalf("%d records got past a failing handler: nothing may be skipped", n)
	}
	released.Store(true)
	waitFor(t, "all 4 in order", func() bool { return len(sink.records()) == 4 })
	perPartitionOrder(t, sink.records())
	if sink.records()[0].Offset != 0 {
		t.Fatalf("the first delivered record must be the one that was blocked, got offset %d", sink.records()[0].Offset)
	}
}

func TestDelivery_ErrorFromTheHandlerIsNeverAnExit(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 3)
	s := service(t)
	var calls atomic.Int64
	var sink batchSink
	h := func(ctx context.Context, b []cdc.Record) error {
		if calls.Add(1) <= 8 {
			return fmt.Errorf("failure %d", calls.Load())
		}
		return sink.handle(ctx, b)
	}
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, h)
	waitFor(t, "recovery after 8 failures", func() bool { return len(sink.records()) == 3 })
}

func TestDelivery_PanicIsRetriedManyTimes(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 2)
	s := service(t)
	var calls atomic.Int64
	var sink batchSink
	h := func(ctx context.Context, b []cdc.Record) error {
		if calls.Add(1) <= 3 {
			panic(fmt.Sprintf("boom %d", calls.Load()))
		}
		return sink.handle(ctx, b)
	}
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, h)
	waitFor(t, "recovery after 3 panics", func() bool { return len(sink.records()) == 2 })
}

func TestDelivery_NilPanicValueIsContainedToo(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 1)
	s := service(t)
	var calls atomic.Int64
	var sink batchSink
	h := func(ctx context.Context, b []cdc.Record) error {
		if calls.Add(1) == 1 {
			var m map[string]int
			//lint:ignore SA5000 the nil map write is the panic under test
			m["x"] = 1 //nolint:staticcheck // a nil map write: a runtime panic, not a string
		}
		return sink.handle(ctx, b)
	}
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, h)
	waitFor(t, "recovery after a runtime panic", func() bool { return len(sink.records()) == 1 })
}

// Stop drains: the batch in hand gets until Stop's own context ends to finish, and
// only then is the handler's context cancelled. A handler that is stuck therefore
// delays Stop by exactly the budget the caller gave it, and no longer.
func TestDelivery_StopGivesAStuckHandlerExactlyItsBudgetThenCancelsIt(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 1)
	s := newService()
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	ended := make(chan struct{})
	var once sync.Once
	h := func(ctx context.Context, _ []cdc.Record) error {
		once.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			close(ended)
			return ctx.Err()
		case <-time.After(60 * time.Second):
			return nil
		}
	}
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, h)
	<-entered
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	err := s.Stop(stopCtx)
	took := time.Since(started)
	if err == nil {
		t.Fatal("Stop reported success although the handler never finished within its budget")
	}
	if took < 1500*time.Millisecond || took > 10*time.Second {
		t.Fatalf("Stop took %v, want about its 2s budget", took)
	}
	select {
	case <-ended:
	case <-time.After(15 * time.Second):
		t.Fatal("the handler's context was never cancelled after Stop gave up waiting")
	}
}

// A handler that finishes within Stop's budget is not interrupted: its context is
// still live while it works, and Stop returns without error.
func TestDelivery_StopLetsAHandlerFinishWithinItsBudget(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 1)
	s := newService()
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	var once sync.Once
	var cancelledEarly atomic.Bool
	var finished atomic.Bool
	h := func(ctx context.Context, _ []cdc.Record) error {
		once.Do(func() { close(entered) })
		time.Sleep(1200 * time.Millisecond) // work that outlasts the call to Stop
		if ctx.Err() != nil {
			cancelledEarly.Store(true)
			return ctx.Err()
		}
		finished.Store(true)
		return nil
	}
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, h)
	<-entered
	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if cancelledEarly.Load() || !finished.Load() {
		t.Fatalf("the handler was interrupted (cancelledEarly=%v, finished=%v): Stop must let work in hand finish",
			cancelledEarly.Load(), finished.Load())
	}
}

func TestDelivery_CommittedBatchIsNotDeliveredToTheNextConsumer(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 10)
	a := newService()
	if err := a.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	var sinkA batchSink
	_ = a.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sinkA.handle)
	waitFor(t, "A to handle all 10", func() bool { return len(sinkA.records()) == 10 })
	time.Sleep(500 * time.Millisecond) // let the commit land
	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := a.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	b := service(t)
	var sinkB batchSink
	_ = b.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sinkB.handle)
	_ = producer(t).Publish(ctxT(t), topic, key(99), event(cdc.OpCreate, 99, "new"))
	waitFor(t, "B to see the new record", func() bool { return len(sinkB.records()) >= 1 })
	for _, r := range sinkB.records() {
		if r.Offset < 10 {
			t.Fatalf("B was handed offset %d, which A had already handled and committed", r.Offset)
		}
	}
}

func TestDelivery_CommitErrorCallbackIsOptional(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 3)
	s := service(t)
	var sink batchSink
	// No OnCommitError, OnAssigned or OnRevoked: all of them are optional.
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "3 records", func() bool { return len(sink.records()) == 3 })
}

func TestDelivery_OnAssignedIsToldTheTopicsPartitions(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 3)
	s := service(t)
	var mu sync.Mutex
	got := map[int32]bool{}
	var sink batchSink
	opts := cdc.BatchOptions{OnAssigned: func(ps []int32) {
		mu.Lock()
		defer mu.Unlock()
		for _, p := range ps {
			got[p] = true
		}
	}}
	_ = s.SubscribeBatch(ctxT(t), topic, opts, sink.handle)
	waitFor(t, "all 3 partitions assigned", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 3
	})
}

// ---- lifecycle -------------------------------------------------------------

func TestDelivery_SubscribeBeforeRunStartsWhenRunIsCalled(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 5)
	s := newService()
	var sink batchSink
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if n := len(sink.records()); n != 0 {
		t.Fatalf("%d records delivered before Run", n)
	}
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	waitFor(t, "5 records after Run", func() bool { return len(sink.records()) == 5 })
}

func TestDelivery_RunTwiceIsHarmless(t *testing.T) {
	t.Parallel()
	skipUnlessKafka(t)
	s := newService()
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatalf("second Run = %v", err)
	}
}

func TestDelivery_StopTwiceIsHarmless(t *testing.T) {
	t.Parallel()
	skipUnlessKafka(t)
	s := newService()
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if err := s.Stop(ctxT(t)); err != nil {
			t.Fatalf("Stop #%d = %v", i+1, err)
		}
	}
}

func TestDelivery_StopWithoutRunIsHarmless(t *testing.T) {
	t.Parallel()
	if err := newService().Stop(ctxT(t)); err != nil {
		t.Fatalf("Stop without Run = %v", err)
	}
}

func TestDelivery_NoDeliveryAfterStop(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s := newService()
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	var sink batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle)
	publishN(t, topic, 3)
	waitFor(t, "3 records", func() bool { return len(sink.records()) == 3 })
	if err := s.Stop(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	_ = producer(t).Publish(ctxT(t), topic, key(50), event(cdc.OpCreate, 50, "late"))
	time.Sleep(800 * time.Millisecond)
	if n := len(sink.records()); n != 3 {
		t.Fatalf("%d records, want 3: nothing may be delivered after Stop", n)
	}
}

func TestDelivery_RestartResumesAfterTheLastCommit(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	publishN(t, topic, 5)
	first := newService()
	if err := first.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	var s1 batchSink
	_ = first.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, s1.handle)
	waitFor(t, "5 records", func() bool { return len(s1.records()) == 5 })
	time.Sleep(500 * time.Millisecond)
	if err := first.Stop(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	second := service(t)
	var s2 batchSink
	_ = second.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, s2.handle)
	publishN(t, topic, 3) // offsets 5, 6, 7
	waitFor(t, "the 3 new records", func() bool { return len(s2.records()) >= 3 })
	if first := s2.records()[0].Offset; first != 5 {
		t.Fatalf("the restarted consumer began at offset %d, want 5", first)
	}
}

func TestDelivery_UnsubscribeKeepsOtherTopicsRunning(t *testing.T) {
	t.Parallel()
	a, b := names(), names()
	createTopic(t, a, 1)
	createTopic(t, b, 1)
	s := service(t)
	var sa, sb batchSink
	_ = s.SubscribeBatch(ctxT(t), a, cdc.BatchOptions{}, sa.handle)
	_ = s.SubscribeBatch(ctxT(t), b, cdc.BatchOptions{}, sb.handle)
	if err := s.Unsubscribe(ctxT(t), a); err != nil {
		t.Fatal(err)
	}
	publishN(t, b, 4)
	publishN(t, a, 4)
	waitFor(t, "topic b to keep flowing", func() bool { return len(sb.records()) == 4 })
	time.Sleep(500 * time.Millisecond)
	if n := len(sa.records()); n != 0 {
		t.Fatalf("an unsubscribed topic delivered %d records", n)
	}
}

func TestDelivery_UnsubscribeUnknownTopicIsIgnored(t *testing.T) {
	t.Parallel()
	s := service(t)
	if err := s.Unsubscribe(ctxT(t), names()); err != nil {
		t.Fatalf("Unsubscribe(unknown) = %v", err)
	}
}

func TestDelivery_ResubscribeAfterUnsubscribeWorks(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s := service(t)
	var s1 batchSink
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, s1.handle)
	publishN(t, topic, 3)
	waitFor(t, "first subscription to see 3", func() bool { return len(s1.records()) == 3 })
	time.Sleep(500 * time.Millisecond)
	if err := s.Unsubscribe(ctxT(t), topic); err != nil {
		t.Fatal(err)
	}
	var s2 batchSink
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, s2.handle); err != nil {
		t.Fatalf("resubscribe = %v", err)
	}
	_ = producer(t).Publish(ctxT(t), topic, key(77), event(cdc.OpCreate, 77, "again"))
	waitFor(t, "the resubscription to see the new record", func() bool { return len(s2.records()) >= 1 })
}

func TestDelivery_DuplicateSubscriptionIsRefused(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	var sink batchSink
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle); err != nil {
		t.Fatal(err)
	}
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle); !errors.Is(err, cdc.ErrDuplicateSubscription) {
		t.Fatalf("second SubscribeBatch = %v", err)
	}
}

func TestDelivery_ManyTopicsAtOnceDoNotMix(t *testing.T) {
	t.Parallel()
	s := service(t)
	const topics = 6
	sinks := make([]*batchSink, topics)
	names6 := make([]string, topics)
	for i := range topics {
		names6[i] = names()
		createTopic(t, names6[i], 1)
		sinks[i] = &batchSink{}
		if err := s.SubscribeBatch(ctxT(t), names6[i], cdc.BatchOptions{}, sinks[i].handle); err != nil {
			t.Fatal(err)
		}
	}
	p := producer(t)
	for i := range topics {
		for j := range 5 + i {
			_ = p.Publish(ctxT(t), names6[i], key(j), event(cdc.OpCreate, j, "x"))
		}
	}
	for i := range topics {
		want := 5 + i
		waitFor(t, fmt.Sprintf("topic %d to see %d", i, want), func() bool { return len(sinks[i].records()) == want })
		for _, r := range sinks[i].records() {
			if r.Topic != names6[i] {
				t.Fatalf("topic %d received a record of %s", i, r.Topic)
			}
		}
	}
}

// ---- the single-record Subscribe -------------------------------------------

func TestSubscribe_ChangeOrderPerRowIsKept(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	p := producer(t)
	for i := range 30 {
		_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpUpdate, 1, fmt.Sprintf("n%02d", i)))
	}
	s := service(t)
	var in inbox
	_ = s.Subscribe(ctxT(t), topic, in.handle)
	msgs := in.wait(t, 30)
	for i, m := range msgs {
		if m.Name != fmt.Sprintf("n%02d", i) {
			t.Fatalf("message %d is %q", i, m.Name)
		}
	}
}

func TestSubscribe_TombstonesAreSkipped(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	p := producer(t)
	_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "a"))
	_ = p.Publish(ctxT(t), topic, key(1), nil)
	_ = p.Publish(ctxT(t), topic, key(2), event(cdc.OpCreate, 2, "b"))
	s := service(t)
	var in inbox
	_ = s.Subscribe(ctxT(t), topic, in.handle)
	msgs := in.wait(t, 2)
	time.Sleep(400 * time.Millisecond)
	if n := len(in.all()); n != 2 {
		t.Fatalf("%d messages, want 2: the tombstone must not reach the handler (%+v)", n, msgs)
	}
}

func TestSubscribe_ExhaustedRetriesParkTheRecordAndMoveOn(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s := service(t)
	var in inbox
	_ = s.Subscribe(ctxT(t), topic, func(ctx context.Context, k json.RawMessage, ev cdc.ChangeEvent) error {
		if strings.Contains(string(ev.After), "bad") {
			return errors.New("always fails")
		}
		return in.handle(ctx, k, ev)
	})
	p := producer(t)
	_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "bad"))
	_ = p.Publish(ctxT(t), topic, key(2), event(cdc.OpCreate, 2, "good"))
	in.wait(t, 1)
	dead := readAll(t, topic+".dlq", 1)
	if header(dead[0], cdc.HeaderDLQAttempts) != "3" {
		t.Fatalf("attempts header = %q, want 3", header(dead[0], cdc.HeaderDLQAttempts))
	}
	if header(dead[0], cdc.HeaderDLQGroup) == "" || header(dead[0], cdc.HeaderDLQFailedAt) == "" {
		t.Fatalf("headers = %+v", dead[0].Headers)
	}
}

func TestSubscribe_PanickingHandlerIsParkedNotFatal(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s := service(t)
	var in inbox
	_ = s.Subscribe(ctxT(t), topic, func(ctx context.Context, k json.RawMessage, ev cdc.ChangeEvent) error {
		if strings.Contains(string(ev.After), "explode") {
			panic("handler bug")
		}
		return in.handle(ctx, k, ev)
	})
	p := producer(t)
	_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "explode"))
	_ = p.Publish(ctxT(t), topic, key(2), event(cdc.OpCreate, 2, "fine"))
	in.wait(t, 1)
	dead := readAll(t, topic+".dlq", 1)
	if header(dead[0], cdc.HeaderDLQError) == "" {
		t.Fatal("the dead letter must say why it was parked")
	}
}

func TestSubscribe_UndecodableValueIsParked(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s := service(t)
	var in inbox
	_ = s.Subscribe(ctxT(t), topic, in.handle)
	p := producer(t)
	_ = p.Publish(ctxT(t), topic, key(1), []byte("this is not json"))
	_ = p.Publish(ctxT(t), topic, key(2), event(cdc.OpCreate, 2, "ok"))
	in.wait(t, 1)
	dead := readAll(t, topic+".dlq", 1)
	if string(dead[0].Value) != "this is not json" {
		t.Fatalf("dead value = %q", dead[0].Value)
	}
}

func TestSubscribe_DeadLetterErrorTextNeverHasTheRow(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s := service(t)
	_ = s.Subscribe(ctxT(t), topic, func(context.Context, json.RawMessage, cdc.ChangeEvent) error {
		return errors.New("downstream rejected the change")
	})
	secret := "SECRET-CARD-4242424242424242"
	_ = producer(t).Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, secret))
	dead := readAll(t, topic+".dlq", 1)[0]
	if strings.Contains(header(dead, cdc.HeaderDLQError), secret) {
		t.Fatalf("the error header leaked the row: %q", header(dead, cdc.HeaderDLQError))
	}
}

// ---- dead letters ----------------------------------------------------------

func TestDeadLetter_KeepsOriginalHeaders(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	rec := cdc.Record{Topic: topic, Partition: 1, Offset: 7, Key: []byte(`{"id":1}`), Value: []byte("v"),
		Headers: []cdc.Header{{Key: "trace-id", Value: []byte("abc123")}}}
	if err := s.DeadLetter(ctxT(t), rec, errors.New("why")); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, topic+".dlq", 1)[0]
	if header(got, "trace-id") != "abc123" {
		t.Fatalf("the original header was lost: %+v", got.Headers)
	}
	if header(got, cdc.HeaderDLQError) != "why" {
		t.Fatalf("dlq headers missing: %+v", got.Headers)
	}
}

func TestDeadLetter_NilCauseIsUnspecified(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	if err := s.DeadLetter(ctxT(t), cdc.Record{Topic: topic, Key: []byte("{}"), Value: []byte("v")}, nil); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, topic+".dlq", 1)[0]
	if header(got, cdc.HeaderDLQError) != "unspecified" {
		t.Fatalf("error header = %q", header(got, cdc.HeaderDLQError))
	}
}

func TestDeadLetter_NilContextIsRefused(t *testing.T) {
	t.Parallel()
	s := service(t)

	if err := s.DeadLetter(nil, cdc.Record{Topic: "t"}, errors.New("x")); !errors.Is(err, cdc.ErrNilContext) {
		t.Fatalf("DeadLetter(nil ctx) = %v", err)
	}
}

func TestDeadLetter_TombstoneRecordKeepsAnEmptyValue(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	if err := s.DeadLetter(ctxT(t), cdc.Record{Topic: topic, Key: []byte(`{"id":5}`)}, errors.New("x")); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, topic+".dlq", 1)[0]
	if string(got.Key) != `{"id":5}` || len(got.Value) != 0 {
		t.Fatalf("key/value = %q / %q", got.Key, got.Value)
	}
}

func TestDeadLetter_ManyConcurrentAllArrive(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	const n = 40
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := cdc.Record{Topic: topic, Partition: 0, Offset: int64(i), Key: key(i), Value: []byte("v")}
			if err := s.DeadLetter(ctxT(t), rec, fmt.Errorf("cause %d", i)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	got := readAll(t, topic+".dlq", n)
	offsets := map[string]bool{}
	for _, r := range got {
		offsets[header(r, cdc.HeaderDLQOffset)] = true
	}
	if len(offsets) != n {
		t.Fatalf("%d distinct origins on the dead-letter topic, want %d", len(offsets), n)
	}
}

func TestDeadLetter_HeaderCarriesTheOriginalTopicEvenWithDots(t *testing.T) {
	t.Parallel()
	topic := names() + ".public.accounts"
	s := service(t)
	if err := s.DeadLetter(ctxT(t), cdc.Record{Topic: topic, Key: []byte("{}"), Value: []byte("v")}, errors.New("x")); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, topic+".dlq", 1)[0]
	if header(got, cdc.HeaderDLQTopic) != topic {
		t.Fatalf("original topic header = %q", header(got, cdc.HeaderDLQTopic))
	}
}
