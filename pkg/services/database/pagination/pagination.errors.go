package pagination

import "errors"

var (
	ErrReadServiceRequired    = errors.New("pagination: ReadSQLService must be set")
	ErrReadDBNotInitialized   = errors.New("pagination: read db is not initialized")
	ErrColumnDefaultIDMissing = errors.New("pagination: ColumnDefaultID must be set")
	ErrInvalidPartitionKey    = errors.New("pagination: partition column must be part of the primary key")

	ErrUnknownField       = errors.New("pagination: unknown field")
	ErrInvalidFilter      = errors.New("pagination: invalid filter")
	ErrInvalidCursor      = errors.New("pagination: invalid cursor")
	ErrRowLockUnsupported = errors.New("pagination: row locking is not supported with mixed-direction cursor pagination")
)
