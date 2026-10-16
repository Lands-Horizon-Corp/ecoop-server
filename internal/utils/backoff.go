package utils

import "time"

// Backoff is the wait after attempts failures: base, doubled each time, up to
// limit.
func Backoff(base time.Duration, attempts int, limit time.Duration) time.Duration {
	d := base
	for i := 0; i < attempts && d < limit; i++ {
		d *= 2
	}
	return min(d, limit)
}
