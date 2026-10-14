//go:build integration

package cdc_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// DeadLetters reads back what DeadLetter parked, against a real Kafka.

func park(t testing.TB, s cdc.ICDCService, topic string, partition int32, offset int64, key, value string) {
	t.Helper()
	rec := cdc.Record{Topic: topic, Partition: partition, Offset: offset, Key: []byte(key), Value: []byte(value)}
	if err := s.DeadLetter(ctxT(t), rec, fmt.Errorf("cause %d", offset)); err != nil {
		t.Fatal(err)
	}
}

func TestDeadLetters_ReadsEverythingParkedWithItsOrigin(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	for i := range 12 {
		park(t, s, topic, int32(i%3), int64(100+i), fmt.Sprintf(`{"id":%d}`, i), fmt.Sprintf("value-%d", i))
	}
	got, err := s.DeadLetters(ctxT(t), topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 12 {
		t.Fatalf("read %d of 12", len(got))
	}
	seen := map[string]bool{}
	for _, r := range got {
		if r.Topic != topic+".dlq" {
			t.Fatalf("topic = %q", r.Topic)
		}
		seen[string(r.Key)] = true
		var origin string
		for _, h := range r.Headers {
			if h.Key == cdc.HeaderDLQOffset {
				origin = string(h.Value)
			}
		}
		if origin == "" {
			t.Fatalf("record %s lost its dlq headers: %+v", r.Key, r.Headers)
		}
	}
	for i := range 12 {
		if !seen[fmt.Sprintf(`{"id":%d}`, i)] {
			t.Fatalf("id %d is missing", i)
		}
	}
}

func TestDeadLetters_NoDeadLetterTopicIsNothing(t *testing.T) {
	t.Parallel()
	s := service(t)
	got, err := s.DeadLetters(ctxT(t), names(), 0)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %d records, err %v", len(got), err)
	}
}

// Reading is not consuming: a second read sees the same records, and asking about
// a topic never creates its dead-letter topic.
func TestDeadLetters_ReadingDoesNotConsume(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	if _, err := s.DeadLetters(ctxT(t), topic, 0); err != nil {
		t.Fatal(err)
	}
	if n := countFor(t, topic+".dlq", 300*time.Millisecond); n != 0 {
		t.Fatalf("asking created data on the topic: %d", n)
	}
	park(t, s, topic, 0, 1, `{"id":1}`, "x")
	park(t, s, topic, 0, 2, `{"id":2}`, "y")
	for range 3 {
		got, err := s.DeadLetters(ctxT(t), topic, 0)
		if err != nil || len(got) != 2 {
			t.Fatalf("got %d records, err %v", len(got), err)
		}
	}
}

func TestDeadLetters_RefusesMoreThanTheLimit(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	for i := range 5 {
		park(t, s, topic, 0, int64(i), fmt.Sprintf(`{"id":%d}`, i), "v")
	}
	if _, err := s.DeadLetters(ctxT(t), topic, 4); !errors.Is(err, cdc.ErrTooManyDeadLetters) {
		t.Fatalf("err = %v, want ErrTooManyDeadLetters", err)
	}
	got, err := s.DeadLetters(ctxT(t), topic, 5)
	if err != nil || len(got) != 5 {
		t.Fatalf("at the limit: got %d, err %v", len(got), err)
	}
}

func TestDeadLetters_ArgumentsAreChecked(t *testing.T) {
	t.Parallel()
	s := newService()
	if _, err := s.DeadLetters(ctxT(t), "", 0); !errors.Is(err, cdc.ErrNoTopic) {
		t.Fatalf("empty topic: %v", err)
	}
	//lint:ignore SA1012 a nil context is the case under test
	if _, err := s.DeadLetters(nil, "t", 0); !errors.Is(err, cdc.ErrNilContext) { //nolint:staticcheck
		t.Fatalf("nil context: %v", err)
	}
	var nilSvc *cdc.CDCService
	if _, err := nilSvc.DeadLetters(ctxT(t), "t", 0); !errors.Is(err, cdc.ErrNoService) {
		t.Fatalf("nil service: %v", err)
	}
	none := cdc.NewCDCService(nil, "g", "p")
	if _, err := none.DeadLetters(ctxT(t), "t", 0); !errors.Is(err, cdc.ErrNoBrokers) {
		t.Fatalf("no brokers: %v", err)
	}
}

// Records arriving while the read is under way are not waited for.
func TestDeadLetters_ReadsInPartitionAndOffsetOrder(t *testing.T) {
	t.Parallel()
	topic := names()
	s := service(t)
	for i := range 30 {
		park(t, s, topic, 0, int64(i), fmt.Sprintf(`{"id":%d}`, i), "v")
	}
	got, err := s.DeadLetters(ctxT(t), topic, 0)
	if err != nil || len(got) != 30 {
		t.Fatalf("got %d, err %v", len(got), err)
	}
	for i := 1; i < len(got); i++ {
		a, b := got[i-1], got[i]
		if a.Partition > b.Partition || (a.Partition == b.Partition && a.Offset >= b.Offset) {
			t.Fatalf("out of order at %d: %d/%d then %d/%d", i, a.Partition, a.Offset, b.Partition, b.Offset)
		}
	}
}
