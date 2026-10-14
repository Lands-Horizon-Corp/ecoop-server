//go:build integration

package cqrs_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// fakeCDC is a cdc.ICDCService that delivers exactly what a test hands it, when
// it hands it, from the test's goroutine. That makes ordering, redelivery,
// overlap between two replicas and failures deterministic, which a real Kafka
// cannot give. Everything the CQRS service relies on is honoured: batches are
// retried until the handler returns nil, only the offsets of a handled batch
// count as committed, and a stopped consumer delivers nothing more.
type fakeCDC struct {
	group string

	mu        sync.Mutex
	handler   cdc.BatchHandler
	opts      cdc.BatchOptions
	topic     string
	committed map[int32]int64 // partition -> next offset Kafka would resume from
	dead      []cdc.Record
	causes    []string
	deadErr   error // when set, DeadLetter fails with it
	// readDeadErr, when set, makes DeadLetters fail with it.
	readDeadErr error
	closed      bool

	deadAttempts atomic.Int64
	deliveries   atomic.Int64
}

var _ cdc.ICDCService = (*fakeCDC)(nil)

func newFakeCDC(group string) *fakeCDC {
	return &fakeCDC{group: group, committed: map[int32]int64{}}
}

func (f *fakeCDC) Run(context.Context) error  { return nil }
func (f *fakeCDC) Stop(context.Context) error { return nil }
func (f *fakeCDC) Topic(schema, table string) string {
	return "test." + schema + "." + table
}
func (f *fakeCDC) GroupID() string { return f.group }
func (f *fakeCDC) Subscribe(context.Context, string, cdc.Handler) error {
	return errors.New("fakeCDC: only batch subscriptions")
}

func (f *fakeCDC) SubscribeBatch(_ context.Context, topic string, opts cdc.BatchOptions, h cdc.BatchHandler) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.handler != nil {
		return cdc.ErrDuplicateSubscription
	}
	f.handler, f.opts, f.topic, f.closed = h, opts, topic, false
	return nil
}

func (f *fakeCDC) Unsubscribe(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handler, f.closed = nil, true
	return nil
}

func (f *fakeCDC) DeadLetter(_ context.Context, rec cdc.Record, cause error) error {
	f.deadAttempts.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deadErr != nil {
		return f.deadErr
	}
	f.dead = append(f.dead, rec)
	f.causes = append(f.causes, cause.Error())
	return nil
}

// DeadLetters is what DeadLetter parked, as the dead-letter topic would hold it:
// each record keeps its key and value (its topic, partition and offset here are
// those of the original, which is all the tests look at).
func (f *fakeCDC) DeadLetters(_ context.Context, _ string, max int) ([]cdc.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readDeadErr != nil {
		return nil, f.readDeadErr
	}
	if max > 0 && len(f.dead) > max {
		return nil, cdc.ErrTooManyDeadLetters
	}
	return append([]cdc.Record(nil), f.dead...), nil
}

func (f *fakeCDC) deadCauses() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.causes...)
}

// failDeadLetter makes DeadLetter fail (nil restores it).
func (f *fakeCDC) failDeadLetter(err error) {
	f.mu.Lock()
	f.deadErr = err
	f.mu.Unlock()
}

func (f *fakeCDC) deadLettered() []cdc.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]cdc.Record(nil), f.dead...)
}

func (f *fakeCDC) committedOffset(partition int32) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.committed[partition]
}

// deliver hands a batch to the subscribed handler and returns what Kafka would
// have done: it retries a failing batch with the same backoff idea as cdc, and
// commits the offsets only once the handler returned nil. It returns ctx.Err()
// if ctx ends first. The handler runs on the caller's goroutine, so a test can
// run several delivers at once to stand in for several replicas.
func (f *fakeCDC) deliver(ctx context.Context, batch []cdc.Record) error {
	f.mu.Lock()
	h := f.handler
	f.mu.Unlock()
	if h == nil {
		return cdc.ErrNotRunning
	}
	for {
		f.deliveries.Add(1)
		if err := h(ctx, batch); err == nil {
			f.mu.Lock()
			last := batch[len(batch)-1]
			if last.Offset+1 > f.committed[last.Partition] {
				f.committed[last.Partition] = last.Offset + 1
			}
			f.mu.Unlock()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// deliverOnce hands a batch over a single time and returns the handler's verdict.
func (f *fakeCDC) deliverOnce(ctx context.Context, batch []cdc.Record) error {
	f.mu.Lock()
	h := f.handler
	f.mu.Unlock()
	if h == nil {
		return cdc.ErrNotRunning
	}
	f.deliveries.Add(1)
	return h(ctx, batch)
}

// rebalance reports partitions revoked and assigned to the service's callbacks.
func (f *fakeCDC) rebalance(revoked, assigned []int32) {
	f.mu.Lock()
	opts := f.opts
	f.mu.Unlock()
	if opts.OnRevoked != nil {
		opts.OnRevoked(revoked)
	}
	if opts.OnAssigned != nil {
		opts.OnAssigned(assigned)
	}
}

func (f *fakeCDC) commitError(partition int32, offset int64, err error) {
	f.mu.Lock()
	opts := f.opts
	f.mu.Unlock()
	if opts.OnCommitError != nil {
		opts.OnCommitError(partition, offset, err)
	}
}

func (f *fakeCDC) subscribed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.handler != nil
}

// --- record builders -------------------------------------------------------

// accountJSON is a full row of the accounts table as Debezium sends it.
func accountJSON(id int64, owner string, balance, version int64) string {
	b, _ := json.Marshal(map[string]any{
		"id": id, "owner": owner, "balance": balance, "version": version,
		"updated_at": "2026-01-02T03:04:05.123456Z",
	})
	return string(b)
}

type recordOpt func(*cdc.Record)

func at(t time.Time) recordOpt { return func(r *cdc.Record) { r.Timestamp = t } }

// lsn says where in the write database's WAL the change sits (source.lsn).
func lsn(n int64) recordOpt {
	return func(r *cdc.Record) {
		if r.Event != nil {
			r.Event.Source.LSN = n
		}
	}
}

func create(partition int32, offset, id int64, owner string, balance, version int64, opts ...recordOpt) cdc.Record {
	return event(partition, offset, cdc.OpCreate, id, "null", accountJSON(id, owner, balance, version), opts...)
}

func update(partition int32, offset, id int64, owner string, balance, version int64, opts ...recordOpt) cdc.Record {
	return event(partition, offset, cdc.OpUpdate, id, "null", accountJSON(id, owner, balance, version), opts...)
}

func remove(partition int32, offset, id int64, opts ...recordOpt) cdc.Record {
	return event(partition, offset, cdc.OpDelete, id, `{"id":`+itoa(id)+`}`, "null", opts...)
}

func tombstone(partition int32, offset, id int64) cdc.Record {
	return cdc.Record{Topic: testTopic, Partition: partition, Offset: offset, Key: json.RawMessage(`{"id":` + itoa(id) + `}`)}
}

func poison(partition int32, offset int64) cdc.Record {
	return cdc.Record{Topic: testTopic, Partition: partition, Offset: offset, Value: []byte("not json"), Err: cdc.ErrNotChangeEvent}
}

func event(partition int32, offset int64, op cdc.ChangeType, id int64, before, after string, opts ...recordOpt) cdc.Record {
	ev := &cdc.ChangeEvent{Op: op, Before: json.RawMessage(before), After: json.RawMessage(after)}
	ev.Source.Table = "accounts"
	r := cdc.Record{
		Topic: testTopic, Partition: partition, Offset: offset,
		Key: json.RawMessage(`{"id":` + itoa(id) + `}`), Event: ev,
		Timestamp: baseTime.Add(time.Duration(offset) * time.Millisecond),
	}
	for _, o := range opts {
		o(&r)
	}
	return r
}

// seq builds n consecutive creates of ids [from, from+n) at offsets starting at start.
func seq(partition int32, start, from int64, n int) []cdc.Record {
	out := make([]cdc.Record, n)
	for i := range n {
		out[i] = create(partition, start+int64(i), from+int64(i), "owner", 100, 1)
	}
	return out
}

func ctxTimeout(t testing.TB, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
