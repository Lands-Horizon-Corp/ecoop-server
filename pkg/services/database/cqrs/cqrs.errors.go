package cqrs

import "errors"

var (
	ErrFromRequestNotSet           = errors.New("cqrs: FromRequest must be set to use *WithValidation methods")
	ErrMessageBrokerNotInitialized = errors.New("cqrs: message broker service is not initialized")
	ErrReadDBNotInitialized        = errors.New("cqrs: read db is not initialized")
	ErrInvalidText                 = errors.New("cqrs: text is not valid UTF-8 or contains a NUL byte")
	ErrNilTx                       = errors.New("cqrs: transaction is nil or was never started")
	ErrWriteDBNotInitialized       = errors.New("cqrs: write db is not initialized")
	ErrWriteDBUnreachable          = errors.New("cqrs: write db is not reachable")
	ErrReadDBUnreachable           = errors.New("cqrs: read db is not reachable")
	ErrUnknownField                = errors.New("cqrs: unknown field")
)
