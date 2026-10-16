package utils

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Earliest and Latest are the timestamps ListOffsets understands as "the first
// offset still held" and "the next offset to be written".
const (
	Earliest int64 = -2
	Latest   int64 = -1
)

// TopicPartitions is the partitions of topic, or none when the topic does not
// exist. Asking never creates the topic.
func TopicPartitions(ctx context.Context, cl *kgo.Client, topic string) ([]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = new(topic)
	req.Topics = append(req.Topics, rt)
	req.AllowAutoTopicCreation = false
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

// ListOffsets asks the brokers for an offset of every partition: Earliest is
// the first one still held, Latest the next one to be written.
func ListOffsets(ctx context.Context, cl *kgo.Client, topic string, parts []int32, at int64) (map[int32]int64, error) {
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
