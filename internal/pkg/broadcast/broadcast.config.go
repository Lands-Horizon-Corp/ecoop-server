package broadcast

import "time"

const (
	defaultWorkers      = 4
	defaultQueueSize    = 1024
	defaultMaxRetries   = 3
	defaultRetryBackoff = 200 * time.Millisecond
	defaultTimeout      = 5 * time.Second
	defaultMaxPayloadKB = 10

	// Pusher protocol limits (Sockudo follows them).
	maxChannelsPerTrigger = 100
	maxNameLength         = 200
)

// Option tunes a BroadcasterService.
type Option func(*BroadcasterService)

// WithSecure talks to the server over HTTPS (required for Pusher cloud, optional for Sockudo).
func WithSecure(secure bool) Option {
	return func(s *BroadcasterService) { s.secure = secure }
}

// WithWorkers sets how many background workers deliver queued Broadcast calls.
func WithWorkers(n int) Option {
	return func(s *BroadcasterService) { s.workers = n }
}

// WithQueueSize sets how many Broadcast events can wait for delivery before
// Broadcast starts returning ErrQueueFull.
func WithQueueSize(n int) Option {
	return func(s *BroadcasterService) { s.queueSize = n }
}

// WithRetries sets how many times a failed delivery is retried (with
// exponential backoff starting at backoff). Only network errors, 5xx and 429
// are retried; other 4xx errors fail immediately.
func WithRetries(n int, backoff time.Duration) Option {
	return func(s *BroadcasterService) { s.maxRetries, s.retryBackoff = n, backoff }
}

// WithTimeout bounds a single HTTP call to the server.
func WithTimeout(d time.Duration) Option {
	return func(s *BroadcasterService) { s.timeout = d }
}

// WithMaxPayloadKB sets the largest payload accepted before sending (Pusher
// allows 10KB; Sockudo's limit is configurable on the server).
func WithMaxPayloadKB(kb int) Option {
	return func(s *BroadcasterService) { s.maxPayload = kb * 1024 }
}
