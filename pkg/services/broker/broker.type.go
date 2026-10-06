package broker

import "context"

type (
	MessageBrokerServices interface {
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
		Publish(ctx context.Context, topic string, key, value []byte) error
		Subscribe(ctx context.Context, topic string, handler func(key, value []byte) error) error
	}

	Message struct {
		Key, Value []byte
		Topic      string
		Partition  int32
		Offset     int64
	}

	BatchBrokerServices interface {
		MessageBrokerServices
		// PublishBatch sends msgs to one topic in a single request and waits for the acknowledgement.
		PublishBatch(ctx context.Context, topic string, msgs []Message) error
		// Enqueue buffers a message and returns at once; the buffer is sent as a batch when it reaches
		// BatchSize or its oldest message has waited BatchWait. It blocks only when MaxPending messages
		// are already waiting. Failed batches are reported to ProducerOptions.OnError.
		Enqueue(ctx context.Context, topic string, key, value []byte) error
		// Flush sends everything enqueued so far without waiting for the batch to fill. Failures are
		// reported to ProducerOptions.OnError, as with any batch.
		Flush(ctx context.Context) error
		// SubscribeBatch hands the handler up to BatchSize messages at a time, or fewer once the first of
		// them has waited BatchWait. The batch is committed only if the handler returns nil, so a failed
		// batch is delivered again whole and the handler must be idempotent.
		SubscribeBatch(ctx context.Context, topic string, handler func(batch []Message) error) error
	}
)
