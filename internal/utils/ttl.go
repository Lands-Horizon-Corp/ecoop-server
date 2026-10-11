package utils

import "time"

func TTLMillis(ttl time.Duration) int64 {
	if ttl > 0 && ttl < time.Millisecond {
		return 1
	}
	return ttl.Milliseconds()
}
