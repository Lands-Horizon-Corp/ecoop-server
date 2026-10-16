package api

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/cloudwego/hertz/pkg/app"
)

type APIService struct {
	port     int
	cache    cache.ICacheService
	security security.ISecurityService
}

// RegisterRoute implements [IAPIService].
func (a *APIService) RegisterRoute(route Route, callback func(ctx context.Context, c *app.RequestContext) error, mid ...app.HandlerFunc) error {
	panic("unimplemented")
}

// RegisterSSR implements [IAPIService].
func (a *APIService) RegisterSSR(route Route, callback func(ctx context.Context, c *app.RequestContext, sendUpdate func(status string, data any, message ...string)) error, mid ...app.HandlerFunc) error {
	panic("unimplemented")
}

// Run implements [IAPIService].
func (a *APIService) Run(ctx context.Context) error {
	panic("unimplemented")
}

// Start implements [IAPIService].
func (a *APIService) Start(ctx context.Context) error {
	panic("unimplemented")
}

// Stop implements [IAPIService].
func (a *APIService) Stop(ctx context.Context) error {
	panic("unimplemented")
}

func NewAPIService(
	port int,
	cache cache.ICacheService,
	security security.ISecurityService,
) IAPIService {
	return &APIService{}
}
