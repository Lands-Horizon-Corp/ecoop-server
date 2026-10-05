package cqrs

import "errors"

var (
	ErrFromRequestNotSet           = errors.New("cqrs: FromRequest must be set to use *WithValidation methods")
	ErrMessageBrokerNotInitialized = errors.New("cqrs: message broker service is not initialized")
	ErrReadDBNotInitialized        = errors.New("cqrs: read db is not initialized")
	ErrUnknownField                = errors.New("cqrs: unknown field")
)
