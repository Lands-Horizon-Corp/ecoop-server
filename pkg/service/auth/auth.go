package auth

import "github.com/Lands-Horizon-Corp/ecoop-server/pkg/service"

type AuthImpl[T service.ClaimWithID] struct {
	name       string
	csrfHeader string
	ssl        bool
}
