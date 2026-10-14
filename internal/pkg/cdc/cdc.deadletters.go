package cdc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DeadLetters implements ICDCService.
func (c *CDCService) DeadLetters(ctx context.Context, topic string, max int) ([]Record, error) {
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
	if max <= 0 {
		max = DefaultMaxDeadLetters
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
	parts, err := topicPartitions(ctx, admin, dlq)
	if err != nil {
		return nil, fmt.Errorf("cdc: reading the layout of %s: %w", dlq, err)
	}
	if len(parts) == 0 {
		return nil, nil
	}
	first, err := listOffsets(ctx, admin, dlq, parts, -2)
	if err != nil {
		return nil, fmt.Errorf("cdc: reading the start offsets of %s: %w", dlq, err)
	}
	last, err := listOffsets(ctx, admin, dlq, parts, -1)
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
	if total > int64(max) {
		return nil, fmt.Errorf("%w: %s holds %d records, the limit is %d", ErrTooManyDeadLetters, dlq, total, max)
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

// topicPartitions is the partitions of topic, or none when the topic does not exist.
func topicPartitions(ctx context.Context, cl *kgo.Client, topic string) ([]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = new(topic)
	req.Topics = append(req.Topics, rt)
	req.AllowAutoTopicCreation = false // asking must never create the topic
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	var parts []int32
	for _, t := range resp.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			if errors.Is(err, kerr.UnknownTopicOrPartition) {
				return nil, nil
			}
			return nil, err
		}
		for _, p := range t.Partitions {
			parts = append(parts, p.Partition)
		}
	}
	slices.Sort(parts)
	return parts, nil
}

// listOffsets asks the brokers for an offset of every partition: -2 is the
// earliest still held, -1 the next one to be written.
func listOffsets(ctx context.Context, cl *kgo.Client, topic string, parts []int32, at int64) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	for _, p := range parts {
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition = p
		rp.Timestamp = at
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	out := make(map[int32]int64, len(parts))
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, fmt.Errorf("partition %d: %w", p.Partition, err)
			}
			out[p.Partition] = p.Offset
		}
	}
	for _, p := range parts {
		if _, ok := out[p]; !ok {
			return nil, fmt.Errorf("partition %d: the broker did not answer", p)
		}
	}
	return out, nil
}
