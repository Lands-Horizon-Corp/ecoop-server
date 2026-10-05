package sql

import "errors"

var (
	ErrNotInitialized = errors.New("sql: database connection is not initialized")
	ErrInvalidSteps   = errors.New("sql: steps must be greater than zero")
)
