package broadcast

import "time"

const (
	healthChannel = "system-health"
	runnerChannel = "test"
	runnerEvent   = "client-test"
	runnerTick    = 100 * time.Microsecond

	// Pusher accepts at most 10 events in one batch request.
	pusherBatchLimit = 10

	defaultFlushInterval = 5 * time.Second
	defaultQueueFactor   = 20 // queue holds this many batches before Broadcast blocks
)
