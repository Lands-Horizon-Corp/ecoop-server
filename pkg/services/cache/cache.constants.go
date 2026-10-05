package cache

import "time"

const (
	dialTimeout = 5 * time.Second
	ioTimeout   = 5 * time.Second
	poolSize    = 20
	scanCount   = 100
	deleteChunk = 500
)
