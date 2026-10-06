package cqrs

import (
	"errors"

	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
)

var (
	ErrFromRequestNotSet           = errors.New("cqrs: FromRequest must be set to use *WithValidation methods")
	ErrMessageBrokerNotInitialized = errors.New("cqrs: message broker service is not initialized")
	ErrReadDBNotInitialized        = errors.New("cqrs: read db is not initialized")
	ErrInvalidText                 = sqlsvc.ErrInvalidText
	ErrUnsafeText                  = sqlsvc.ErrUnsafeText
	ErrNilTx                       = sqlsvc.ErrNilTx
	ErrWriteDBNotInitialized       = errors.New("cqrs: write db is not initialized")
	ErrWriteDBUnreachable          = errors.New("cqrs: write db is not reachable")
	ErrReadDBUnreachable           = errors.New("cqrs: read db is not reachable")
	ErrUnknownField                = errors.New("cqrs: unknown field")
)
