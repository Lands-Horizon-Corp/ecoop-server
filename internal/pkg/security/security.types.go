package security

import (
	"context"
	"errors"
	"time"
)

var (
	ErrDecrypt       = errors.New("security: cannot decrypt data")
	ErrInvalidConfig = errors.New("security: invalid password hashing config")
	ErrInvalidHash   = errors.New("security: invalid hash format")
)

const (
	hashAlgorithm = "argon2id"
	minSaltLength = 16 // bytes, RFC 9106 recommendation
	minKeyLength  = 16 // bytes
)

const (
	maxVerifyMemory      uint32 = 256 * 1024 // KiB
	maxVerifyIterations  uint32 = 16
	maxVerifyParallelism uint8  = 64
)

type (
	// Option configures a SecurityService: WithObserver.
	Option func(*SecurityService)

	ISecurityService interface {
		GeneratePassword(ctx context.Context, password string) (string, error)
		VerifyPassword(ctx context.Context, hash string, password string) (bool, error)
		Encrypt(ctx context.Context, data string, ttl time.Duration) (string, error)
		Decrypt(ctx context.Context, tokenString string) (string, error)
		EncryptData(ctx context.Context, data []byte) ([]byte, error)
		DecryptData(ctx context.Context, ciphertext []byte) ([]byte, error)
	}
)
