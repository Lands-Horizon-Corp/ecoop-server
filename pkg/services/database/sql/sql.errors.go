package sql

import "errors"

var (
	ErrNotInitialized    = errors.New("sql: database connection is not initialized")
	ErrInvalidSteps      = errors.New("sql: steps must be greater than zero")
	ErrTooManySteps      = errors.New("sql: more steps requested than migrations available")
	ErrNoModels          = errors.New("sql: at least one model is required")
	ErrInvalidName       = errors.New("sql: migration name must contain a letter or digit")
	ErrPendingMigrations = errors.New("sql: apply pending migrations before generating a new one")
	ErrInvalidMigration  = errors.New("sql: generated migration is not valid")
)
