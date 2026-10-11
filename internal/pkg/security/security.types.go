package security

import (
	"context"
	"errors"
	"time"
)

var (
	ErrDecrypt = errors.New("security: cannot decrypt data")
)

const (
	maxVerifyMemory      uint32 = 256 * 1024 // KiB
	maxVerifyIterations  uint32 = 16
	maxVerifyParallelism uint8  = 64
)

type (
	SecurityServices interface {
		GeneratePassword(ctx context.Context, password string) (string, error)
		VerifyPassword(ctx context.Context, hash string, password string) (bool, error)
		Encrypt(ctx context.Context, data string, ttl time.Duration) (string, error)
		Decrypt(ctx context.Context, tokenString string) (string, error)
		EncryptData(ctx context.Context, data []byte) ([]byte, error)
		DecryptData(ctx context.Context, ciphertext []byte) ([]byte, error)
	}
)
