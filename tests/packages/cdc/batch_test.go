//go:build integration

package cdc_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// Tests of SubscribeBatch against a real Kafka: what the CQRS service builds on.

// batchSink collects every record of every batch, and the batches themselves.
type batchSink struct {
	mu      sync.Mutex
	batches [][]cdc.Record
}

func (b *batchSink) handle(_ context.Context, batch []cdc.Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batches = append(b.batches, append([]cdc.Record(nil), batch...))
	return nil
}

func (b *batchSink) records() []cdc.Record {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []cdc.Record
	for _, batch := range b.batches {
		out = append(out, batch...)
	}
	return out
}

func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("never saw %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// createTopic creates topic with the given number of partitions. Producing to a
// missing topic would create it with the broker's default (one partition).
func createTopic(t testing.TB, topic string, partitions int) {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	req := kmsg.NewCreateTopicsRequest()
	req.TimeoutMillis = 15000
	spec := kmsg.NewCreateTopicsRequestTopic()
	spec.Topic, spec.NumPartitions, spec.ReplicationFactor = topic, int32(partitions), 1
	req.Topics = append(req.Topics, spec)
	resp, err := req.RequestWith(context.Background(), c)
	if err != nil {
		t.Fatalf("creating %s: %v", topic, err)
	}
	for _, r := range resp.Topics {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			t.Fatalf("creating %s: %v", topic, err)
		}
	}
}

// Records arrive in batches, in order, with their partition and offset, and a
// tombstone and a poison record are told apart from a change.
func TestBatch_DeliversWithPositionAndKinds(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s := service(t)
	var sink batchSink
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{MaxBatch: 100}, sink.handle); err != nil {
		t.Fatal(err)
	}
	p := producer(t)
	_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "a"))
	_ = p.Publish(ctxT(t), topic, key(1), event(cdc.OpUpdate, 1, "b"))
	_ = p.Publish(ctxT(t), topic, key(1), nil)
	_ = p.Publish(ctxT(t), topic, key(2), []byte("not json"))
	waitFor(t, "4 records", func() bool { return len(sink.records()) == 4 })
	got := sink.records()
	for i, r := range got {
		if r.Partition != 0 || r.Offset != int64(i) || r.Topic != topic {
			t.Fatalf("record %d position = %s[%d]@%d", i, r.Topic, r.Partition, r.Offset)
		}
	}
	if got[0].Event == nil || got[0].Event.Op != cdc.OpCreate || got[1].Event.Op != cdc.OpUpdate {
		t.Fatalf("changes: %+v %+v", got[0], got[1])
	}
	if got[2].Event != nil || got[2].Err != nil {
		t.Fatalf("a tombstone has neither event nor error: %+v", got[2])
	}
	if !errors.Is(got[3].Err, cdc.ErrNotChangeEvent) || got[3].Event != nil {
		t.Fatalf("poison must carry the error: %+v", got[3])
	}
	if string(got[3].Value) != "not json" {
		t.Fatalf("the raw value must be kept for the dead letter: %q", got[3].Value)
	}
}

// A handler that fails is called again with the same batch, blocking the
// partition (nothing is skipped), until it succeeds; only then is it committed.
func TestBatch_FailingHandlerIsRetriedNotSkipped(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s := service(t)
	var calls atomic.Int64
	var sink batchSink
	h := func(ctx context.Context, batch []cdc.Record) error {
		if calls.Add(1) < 4 {
			return errors.New("not yet")
		}
		return sink.handle(ctx, batch)
	}
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, h); err != nil {
		t.Fatal(err)
	}
	p := producer(t)
	for i := range 3 {
		_ = p.Publish(ctxT(t), topic, key(i), event(cdc.OpCreate, i, "x"))
	}
	waitFor(t, "the 3 records after retries", func() bool { return len(sink.records()) >= 3 })
	if calls.Load() < 4 {
		t.Fatalf("handler calls = %d, want at least 4", calls.Load())
	}
}

// A handler that panics is contained and retried like any other failure.
func TestBatch_PanickingHandlerIsContained(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s := service(t)
	var calls atomic.Int64
	var sink batchSink
	h := func(ctx context.Context, batch []cdc.Record) error {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		return sink.handle(ctx, batch)
	}
	if err := s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, h); err != nil {
		t.Fatal(err)
	}
	_ = producer(t).Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "x"))
	waitFor(t, "the record after the panic", func() bool { return len(sink.records()) == 1 })
}

// Commits are only issued for handled batches: a consumer that dies before its
// handler succeeds leaves the records for the next one in the group.
func TestBatch_NotHandledMeansNotCommitted(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	p := producer(t)
	for i := range 5 {
		_ = p.Publish(ctxT(t), topic, key(i), event(cdc.OpCreate, i, "x"))
	}
	a := newService()
	if err := a.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	var tried atomic.Int64
	_ = a.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, func(context.Context, []cdc.Record) error {
		tried.Add(1)
		return errors.New("never")
	})
	waitFor(t, "a first attempt", func() bool { return tried.Load() > 0 })
	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = a.Stop(stopCtx)

	b := service(t)
	var sink batchSink
	_ = b.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sink.handle)
	waitFor(t, "the 5 records on the next consumer", func() bool { return len(sink.records()) >= 5 })
	if sink.records()[0].Offset != 0 {
		t.Fatalf("the new consumer must start from the first unhandled record, got offset %d", sink.records()[0].Offset)
	}
}

// Two consumers in one group split a multi-partition topic; when one leaves,
// the other takes over its partitions and every record is seen.
func TestBatch_GroupRebalanceSharesAndFailsOver(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 4)
	p := producer(t)
	publish := func(from, n int) {
		for i := from; i < from+n; i++ {
			_ = p.Publish(ctxT(t), topic, key(i), event(cdc.OpCreate, i, "x"))
		}
	}

	var aRevoked atomic.Int64
	a := newService()
	if err := a.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	var sinkA, sinkB batchSink
	_ = a.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{
		OnRevoked: func(ps []int32) { aRevoked.Add(int64(len(ps))) },
	}, sinkA.handle)
	publish(0, 40)
	waitFor(t, "A to see the first wave", func() bool { return len(sinkA.records()) == 40 })

	b := service(t)
	_ = b.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, sinkB.handle)
	// Keep publishing until B has been handed some partitions.
	n := 40
	waitFor(t, "B to share the stream", func() bool {
		publish(n, 20)
		n += 20
		time.Sleep(300 * time.Millisecond)
		return len(sinkB.records()) > 0
	})
	if aRevoked.Load() == 0 {
		t.Fatal("A must have been told it lost partitions when B joined")
	}

	// B leaves; whatever is published afterwards must still be seen by A.
	if err := b.Unsubscribe(ctxT(t), topic); err != nil {
		t.Fatal(err)
	}
	before := len(sinkA.records())
	publish(n, 30)
	waitFor(t, "A to take over", func() bool { return len(sinkA.records()) >= before+30 })

	seen := map[int64]bool{}
	for _, sink := range []*batchSink{&sinkA, &sinkB} {
		for _, r := range sink.records() {
			seen[int64(r.Partition)*1_000_000+r.Offset] = true
		}
	}
	if total := n + 30; len(seen) < total {
		t.Fatalf("distinct records seen = %d, want %d: a record was lost in the rebalance", len(seen), total)
	}
}

// Stop does not hang when a batch handler is failing forever.
func TestBatch_StopWithAFailingHandlerDoesNotHang(t *testing.T) {
	t.Parallel()
	topic := names()
	createTopic(t, topic, 1)
	s := newService()
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	var tried atomic.Int64
	_ = s.SubscribeBatch(ctxT(t), topic, cdc.BatchOptions{}, func(context.Context, []cdc.Record) error {
		tried.Add(1)
		return errors.New("always")
	})
	_ = producer(t).Publish(ctxT(t), topic, key(1), event(cdc.OpCreate, 1, "x"))
	waitFor(t, "a first attempt", func() bool { return tried.Load() > 0 })
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		done <- s.Stop(ctx)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stop = %v", err)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("Stop hung on a failing handler")
	}
}

// DeadLetter parks a record with the headers that say where it came from.
func TestBatch_DeadLetterCarriesItsOrigin(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	rec := cdc.Record{Topic: topic, Partition: 2, Offset: 41, Key: []byte(`{"id":9}`), Value: []byte("bad")}
	if err := s.DeadLetter(ctxT(t), rec, errors.New("because")); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, topic+".dlq", 1)[0]
	if header(got, cdc.HeaderDLQPartition) != "2" || header(got, cdc.HeaderDLQOffset) != "41" ||
		header(got, cdc.HeaderDLQError) != "because" || header(got, cdc.HeaderDLQTopic) != topic {
		t.Fatalf("headers = %+v", got.Headers)
	}
	if string(got.Value) != "bad" || string(got.Key) != `{"id":9}` {
		t.Fatalf("key/value = %q / %q", got.Key, got.Value)
	}
	if err := newService().DeadLetter(ctxT(t), rec, errors.New("x")); !errors.Is(err, cdc.ErrNotRunning) {
		t.Fatalf("DeadLetter on a service that is not running = %v", err)
	}
}

// The batch API's guards: nil, missing and duplicate arguments.
func TestBatch_RefusesBadArguments(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := func(context.Context, []cdc.Record) error { return nil }
	if err := (&cdc.CDCService{}).SubscribeBatch(ctx, "t", cdc.BatchOptions{}, h); !errors.Is(err, cdc.ErrNoService) {
		t.Errorf("zero service = %v", err)
	}
	s := cdc.NewCDCService(nil, "g", "")
	if err := s.SubscribeBatch(ctx, "", cdc.BatchOptions{}, h); !errors.Is(err, cdc.ErrNoTopic) {
		t.Errorf("empty topic = %v", err)
	}
	if err := s.SubscribeBatch(ctx, "t", cdc.BatchOptions{}, nil); !errors.Is(err, cdc.ErrNoBatchHandler) {
		t.Errorf("nil handler = %v", err)
	}
	if err := cdc.NewCDCService(nil, "", "").SubscribeBatch(ctx, "t", cdc.BatchOptions{}, h); !errors.Is(err, cdc.ErrNoGroup) {
		t.Errorf("no group = %v", err)
	}
	var nilCtx context.Context
	if err := s.SubscribeBatch(nilCtx, "t", cdc.BatchOptions{}, h); !errors.Is(err, cdc.ErrNilContext) {
		t.Errorf("nil ctx = %v", err)
	}
	if err := s.SubscribeBatch(ctx, "t", cdc.BatchOptions{}, h); err != nil {
		t.Fatal(err)
	}
	if err := s.SubscribeBatch(ctx, "t", cdc.BatchOptions{}, h); !errors.Is(err, cdc.ErrDuplicateSubscription) {
		t.Errorf("duplicate = %v", err)
	}
	if err := s.Unsubscribe(ctx, "t"); err != nil {
		t.Errorf("Unsubscribe = %v", err)
	}
	if err := s.Unsubscribe(ctx, "never-subscribed"); err != nil {
		t.Errorf("Unsubscribe of an unknown topic must be a no-op: %v", err)
	}
	var ptr *cdc.CDCService
	if ptr.GroupID() != "" {
		t.Error("nil GroupID")
	}
}
