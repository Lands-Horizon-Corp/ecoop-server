//go:build integration

package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// readDLQ returns the first record on topic, read by a throwaway group.
func readDLQ(t *testing.T, topic string) *kgo.Record {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for {
		fetches := c.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("nothing arrived on %s", topic)
		}
		var first *kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			if first == nil {
				first = r
			}
		})
		if first != nil {
			return first
		}
	}
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// readAll collects n records from topic, read by a throwaway client.
func readAll(t *testing.T, topic string, n int) []*kgo.Record {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var got []*kgo.Record
	for len(got) < n {
		c.PollFetches(ctx).EachRecord(func(r *kgo.Record) { got = append(got, r) })
		if ctx.Err() != nil {
			t.Fatalf("%s holds %d of %d records", topic, len(got), n)
		}
	}
	return got
}

// countFor returns how many records topic holds after waiting d for more.
func countFor(t *testing.T, topic string, d time.Duration) int {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	n := 0
	for ctx.Err() == nil {
		c.PollFetches(ctx).EachRecord(func(*kgo.Record) { n++ })
	}
	return n
}
