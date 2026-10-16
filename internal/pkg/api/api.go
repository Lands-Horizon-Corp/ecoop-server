package api

import (
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
)

type APIService struct {
	port     int
	cache    cache.ICacheService
	security security.ISecurityService
}

func NewAPIService(
	port int,
	cache cache.ICacheService,
	security security.ISecurityService,
) IAPIService {
	return &APIService{}
}
