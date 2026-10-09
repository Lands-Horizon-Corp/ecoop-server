package broker

import (
	"time"

	"github.com/IBM/sarama"
)

const (
	defaultMaxRetries   = 3
	defaultRetryBackoff = 200 * time.Millisecond
	reconnectBackoff    = time.Second
	dlqSuffix           = ".dlq"
)

// Option tunes a BrokerService.
type Option func(*BrokerService)

// WithRetries sets how many times a failing handler is retried (with
// exponential backoff starting at backoff) before the message goes to the DLQ.
func WithRetries(n int, backoff time.Duration) Option {
	return func(s *BrokerService) {
		s.maxRetries, s.retryBackoff = n, backoff
	}
}

// WithConfig lets callers adjust the sarama config (TLS, SASL, version, ...).
func WithConfig(fn func(*sarama.Config)) Option {
	return func(s *BrokerService) { fn(s.cfg) }
}

// NewSaramaConfig returns the defaults BrokerService uses: acks from all
// replicas, idempotent producer (no duplicates on producer retry), new consumer
// groups start from the oldest offset, and consumer errors are reported.
func NewSaramaConfig(clientID string) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = clientID
	cfg.Version = sarama.V3_6_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1 // required by the idempotent producer
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true
	return cfg
}
