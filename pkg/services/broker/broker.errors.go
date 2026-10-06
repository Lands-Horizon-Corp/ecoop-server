package broker

import "errors"

var (
	ErrNoBrokers  = errors.New("broker: at least one Kafka broker address is required")
	ErrNoGroup    = errors.New("broker: a consumer group id is required to subscribe")
	ErrNotRunning = errors.New("broker: not running; call Run first")
)
