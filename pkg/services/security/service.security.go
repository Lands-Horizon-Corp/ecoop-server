package security

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"aidanwoods.dev/go-paseto"
	"golang.org/x/crypto/argon2"
)

type SecurityService struct {
	secret      string
	memory      uint32
	iterations  uint32
	parallelism uint8
	salt        uint32
	key         uint32
}

func NewSecurityService(
	secret string, memory uint32, iterations uint32, parallelism uint8, salt uint32, key uint32) SecurityServices {
	return &SecurityService{
		secret:      secret,
		memory:      memory,
		iterations:  iterations,
		parallelism: parallelism,
		salt:        salt,
		key:         key,
	}
}

func (s *SecurityService) pasetoKey() paseto.V4SymmetricKey {
	hash := sha256.Sum256([]byte(s.secret))
	key, _ := paseto.V4SymmetricKeyFromBytes(hash[:])
	return key
}

func (s *SecurityService) GeneratePassword(ctx context.Context, password string) (string, error) {
	salt := make([]byte, s.salt)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("failed to generate salt: %w", err)
	}
	hash := argon2.IDKey([]byte(password), salt, s.iterations, s.memory, s.parallelism, s.key)
	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version, s.memory, s.iterations, s.parallelism, b64Salt, b64Hash), nil
}

func (s *SecurityService) VerifyPassword(ctx context.Context, hash string, password string) (bool, error) {
	vals := strings.Split(hash, "$")
	if len(vals) != 6 {
		return false, fmt.Errorf("invalid hash format")
	}
	var m, t uint32
	var p uint8
	if _, err := fmt.Sscanf(vals[3], "m=%d,t=%d,p=%d", &m, &t, &p); err != nil {
		return false, err
	}
	// argon2.IDKey panics on zero rounds or parallelism.
	if t < 1 || p < 1 {
		return false, fmt.Errorf("invalid hash parameters")
	}
	// The hash supplies its own cost, so cap it to stop a crafted hash exhausting memory or CPU.
	if m > maxVerifyMemory || t > maxVerifyIterations || p > maxVerifyParallelism {
		return false, fmt.Errorf("hash parameters exceed allowed limits")
	}
	salt, err := base64.RawStdEncoding.Strict().DecodeString(vals[4])
	if err != nil {
		return false, err
	}
	expectedHash, err := base64.RawStdEncoding.Strict().DecodeString(vals[5])
	if err != nil {
		return false, err
	}
	if subtle.ConstantTimeCompare(
		expectedHash, argon2.IDKey([]byte(password), salt, t, m, p, s.key)) == 1 {
		return true, nil
	}
	return false, nil
}

func (s *SecurityService) Encrypt(ctx context.Context, data string, ttl time.Duration) (string, error) {
	token := paseto.NewToken()
	token.SetIssuedAt(time.Now())
	token.SetNotBefore(time.Now())
	token.SetExpiration(time.Now().Add(ttl))
	token.SetString("payload", data)
	encrypted := token.V4Encrypt(s.pasetoKey(), nil)
	return encrypted, nil
}

func (s *SecurityService) Decrypt(ctx context.Context, tokenString string) (string, error) {
	parser := paseto.NewParser()
	parser.AddRule(paseto.NotExpired())
	parser.AddRule(paseto.ValidAt(time.Now()))
	token, err := parser.ParseV4Local(s.pasetoKey(), tokenString, nil)
	if err != nil {
		return "", fmt.Errorf("token validation failed: %w", err)
	}
	payload, err := token.GetString("payload")
	if err != nil {
		return "", fmt.Errorf("failed to extract payload from token: %w", err)
	}
	return payload, nil
}
