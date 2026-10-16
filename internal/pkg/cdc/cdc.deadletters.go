package cdc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// DeadLetters implements ICDCService.
func (c *CDCService) DeadLetters(ctx context.Context, topic string, limit int) ([]Record, error) {
	if c == nil || c.subs == nil {
		return nil, ErrNoService
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	if strings.TrimSpace(topic) == "" {
		return nil, ErrNoTopic
	}
	if len(c.brokers) == 0 {
		return nil, ErrNoBrokers
	}
	if limit <= 0 {
		limit = DefaultMaxDeadLetters
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, deadLettersTimeout)
		defer cancel()
	}
	dlq := topic + c.dlqSuffix

	// What is on the topic is decided first, from the brokers' own offsets: the
	// partitions, and for each where it starts and where it ends now. The reader
	// then stops at those ends instead of waiting for a quiet moment.
	admin, err := kgo.NewClient(kgo.SeedBrokers(c.brokers...))
	if err != nil {
		return nil, errors.Join(ErrConnect, err)
	}
	defer admin.Close()
	parts, err := utils.TopicPartitions(ctx, admin, dlq)
	if err != nil {
		return nil, fmt.Errorf("cdc: reading the layout of %s: %w", dlq, err)
	}
	if len(parts) == 0 {
		return nil, nil
	}
	first, err := utils.ListOffsets(ctx, admin, dlq, parts, utils.Earliest)
	if err != nil {
		return nil, fmt.Errorf("cdc: reading the start offsets of %s: %w", dlq, err)
	}
	last, err := utils.ListOffsets(ctx, admin, dlq, parts, utils.Latest)
	if err != nil {
		return nil, fmt.Errorf("cdc: reading the end offsets of %s: %w", dlq, err)
	}
	from := map[int32]kgo.Offset{}
	end := map[int32]int64{} // partitions still being read, and where each ends
	total := int64(0)
	for _, p := range parts {
		if last[p] > first[p] {
			from[p] = kgo.NewOffset().At(first[p])
			end[p] = last[p]
			total += last[p] - first[p]
		}
	}
	if total == 0 {
		return nil, nil
	}
	if total > int64(limit) {
		return nil, fmt.Errorf("%w: %s holds %d records, the limit is %d", ErrTooManyDeadLetters, dlq, total, limit)
	}

	reader, err := kgo.NewClient(kgo.SeedBrokers(c.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{dlq: from}))
	if err != nil {
		return nil, errors.Join(ErrConnect, err)
	}
	defer reader.Close()
	out := make([]Record, 0, total)
	for len(end) > 0 {
		fetches := reader.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("cdc: reading %s (%d of %d records read): %w", dlq, len(out), total, err)
		}
		if err := fetches.Err0(); err != nil {
			return nil, fmt.Errorf("cdc: reading %s: %w", dlq, err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			limit, reading := end[r.Partition]
			if !reading || r.Offset >= limit {
				return
			}
			out = append(out, toRecord(r))
			if r.Offset >= limit-1 {
				delete(end, r.Partition)
			}
		})
	}
	slices.SortFunc(out, func(a, b Record) int {
		if a.Partition != b.Partition {
			return int(a.Partition - b.Partition)
		}
		switch {
		case a.Offset < b.Offset:
			return -1
		case a.Offset > b.Offset:
			return 1
		}
		return 0
	})
	return out, nil
}
