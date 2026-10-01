package security

import (
	"context"
	"time"
)

type SecurityServices interface {
	GeneratePassword(ctx context.Context, password string) (string, error)
	VerifyPassword(ctx context.Context, hash string, password string) (bool, error)
	Encrypt(ctx context.Context, data string, ttl time.Duration) (string, error)
	Decrypt(ctx context.Context, tokenString string) (string, error)
}
