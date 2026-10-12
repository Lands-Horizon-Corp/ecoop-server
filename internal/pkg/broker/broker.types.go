package broker

import (
	"context"
	"errors"
	"time"
)

const (
	HandlerAttempts = 3                      // tries per message before it is skipped
	RetryBackoff    = 200 * time.Millisecond // between tries, doubling
)

var (
	ErrNotRunning            = errors.New("broker: not running")
	ErrNoTracker             = errors.New("broker: service has no in-flight tracker; create it with NewBrokerService")
	ErrNoBrokers             = errors.New("broker: no broker addresses")
	ErrNoGroup               = errors.New("broker: Subscribe needs a consumer group id")
	ErrNoTopic               = errors.New("broker: topic is empty")
	ErrNoHandler             = errors.New("broker: handler is nil")
	ErrDuplicateSubscription = errors.New("broker: topic already subscribed")
	ErrConnect               = errors.New("broker: cannot reach Kafka")
	ErrPublish               = errors.New("broker: publish failed")
)

type (
	// Handler processes one message. Return nil when done; an error retries
	// it (HandlerAttempts in all) before it is skipped.
	Handler func(ctx context.Context, key, value []byte) error

	// BrokerServices publishes to and consumes from Kafka.
	BrokerServices interface {
		// Run connects and starts every subscription; Stop waits for the
		// handlers running, then disconnects. Messages not yet handled are
		// delivered again after the next Run (here or on another instance).
		Run(ctx context.Context) error
		Stop(ctx context.Context) error

		// Publish sends one message and waits until Kafka has it.
		Publish(ctx context.Context, topic string, key, value []byte) error

		// Subscribe consumes topic in the service's consumer group, before
		// or after Run: each message goes to one instance of the group, at
		// least once (committed only after its handler finishes).
		Subscribe(ctx context.Context, topic string, handler func(ctx context.Context, key, value []byte) error) error
	}
)
