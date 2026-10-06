package broker

import (
	"math"
	"time"
)

// Options tunes how much the broker sends and reads at once and how fast. The zero value gives
// sensible defaults: batches of 100 or whatever has waited 5 seconds, no rate limit.
type Options struct {
	Producer ProducerOptions
	Consumer ConsumerOptions
}

// ProducerOptions controls Enqueue and PublishBatch.
//
// Enqueue buffers messages and sends them as one request when BatchSize messages are waiting, or when
// the oldest has waited BatchWait, whichever comes first. A quiet period therefore still delivers a
// short batch instead of holding messages back.
type ProducerOptions struct {
	// BatchSize is the most messages sent in one request. Default 100.
	BatchSize int
	// BatchWait is the longest a buffered message waits for its batch to fill. Default 5s.
	BatchWait time.Duration
	// MaxPending is how many messages Enqueue buffers before it blocks the caller. This is the
	// backpressure: a slow broker slows producers instead of exhausting memory. Default 10 x BatchSize.
	MaxPending int
	// Rate caps messages sent per second (a token bucket). 0 means unlimited.
	Rate float64
	// Burst is the most messages that may go out at once after a quiet period. Default: the larger
	// of BatchSize and Rate. Only used when Rate is set.
	Burst int
	// OnError receives the messages of a batch that could not be delivered, so they can be retried or
	// stored. Enqueue returns before delivery, so this is the only way to learn of a failed batch.
	OnError func(topic string, msgs []Message, err error)
}

// ConsumerOptions controls SubscribeBatch and Subscribe.
type ConsumerOptions struct {
	// BatchSize is the most messages handed to the handler at once. Default 100.
	BatchSize int
	// BatchWait is how long a partial batch waits for more messages before it is handled anyway,
	// counted from its first message. Default 5s.
	BatchWait time.Duration
	// Rate caps messages handed to handlers per second across all subscriptions of this broker.
	// 0 means unlimited.
	Rate float64
	// Burst is the most messages handled at once after a quiet period. Default: the larger of
	// BatchSize and Rate. A batch never exceeds it.
	Burst int
}

func (o Options) withDefaults() Options {
	p := &o.Producer
	if p.BatchSize <= 0 {
		p.BatchSize = 100
	}
	if p.BatchWait <= 0 {
		p.BatchWait = 5 * time.Second
	}
	if p.MaxPending <= 0 {
		p.MaxPending = 10 * p.BatchSize
	}
	if p.Rate > 0 && p.Burst <= 0 {
		p.Burst = max(p.BatchSize, int(math.Ceil(p.Rate)))
	}

	c := &o.Consumer
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.BatchWait <= 0 {
		c.BatchWait = 5 * time.Second
	}
	if c.Rate > 0 {
		if c.Burst <= 0 {
			c.Burst = max(c.BatchSize, int(math.Ceil(c.Rate)))
		}
		c.BatchSize = min(c.BatchSize, c.Burst)
	}
	return o
}
