package api

import (
	"context"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
)

// Errors returned by the service.
var (
	ErrNotStarted     = errors.New("api: service not started")
	ErrAlreadyStarted = errors.New("api: service already started")
	ErrAfterStart     = errors.New("api: routes cannot be registered after Start")
	ErrNilHandler     = errors.New("api: handler is nil")
	ErrInvalidRoute   = errors.New("api: invalid route")
	ErrDuplicateRoute = errors.New("api: route already registered")
)

// PublicError is an error whose code is meant for the client. Anything else a
// handler returns is answered as a generic 500 with the request id, and logged.
type PublicError struct {
	Status int
	Code   string
}

func (e *PublicError) Error() string { return e.Code }

// Public builds a PublicError, for a handler to say "400 invalid_amount".
func Public(status int, code string) error { return &PublicError{Status: status, Code: code} }

// errServerFault ends an observed request that failed on the server: a 5xx, or
// a panic. It carries no detail on purpose; the detail belongs in a log.
var errServerFault = errors.New("api: server error")

// errCheckPanicked is what a readiness check that panicked, or has no function,
// reports. It never reaches a client.
var errCheckPanicked = errors.New("api: readiness check failed")

type (
	Route struct {
		Route        string
		Request      string
		Response     string
		RequestType  any
		ResponseType any
		Method       string
		Note         string
		Private      bool
	}

	IAPIService interface {
		Start(ctx context.Context) error
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
		RegisterRoute(route Route,
			callback func(ctx context.Context, c *app.RequestContext) error,
			mid ...app.HandlerFunc) error
		RegisterSSR(route Route,
			callback func(ctx context.Context, c *app.RequestContext, sendUpdate func(status string, data any, message ...string)) error,
			mid ...app.HandlerFunc) error
	}
)
