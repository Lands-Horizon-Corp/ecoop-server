package api

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
)

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
