package database

import "errors"

var (
	ErrNilService           = errors.New("database: service is nil")
	ErrAlreadyStarted       = errors.New("database: models must be registered before Start")
	ErrNotStarted           = errors.New("database: service has not been started")
	ErrAlreadyRegistered    = errors.New("database: model is already registered")
	ErrNotRegistered        = errors.New("database: model is not registered")
	ErrTypeMismatch         = errors.New("database: registered model has different type parameters")
	ErrMigrateThroughPooler = errors.New("database: migrations must run against Postgres directly, not through PgBouncer")
	ErrSchemaMismatch       = errors.New("database: writer and reader are on different schema versions")
)
