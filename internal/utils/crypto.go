package utils

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"math/big"
)

// RandomDigits returns n uniformly random decimal digits from a
// cryptographically secure source, e.g. for one-time codes.
func RandomDigits(n int) (string, error) {
	b := make([]byte, n)
	for i := range b {
		d, err := rand.Int(rand.Reader, big.NewInt(10))
		if err != nil {
			return "", err
		}
		b[i] = byte('0' + d.Int64())
	}
	return string(b), nil
}

// Pepper mixes a server-side secret into a value before it is hashed, so a
// leaked hash cannot be brute-forced (short codes have few possible values)
// without the secret too.
func Pepper(secret, value string) string {
	return secret + ":" + value
}

// RandomToken returns n random bytes from a cryptographically secure
// source as unpadded base64url: safe in URLs, headers and cookies, e.g.
// for session tokens.
func RandomToken(n int) string {
	return base64.RawURLEncoding.EncodeToString(randomBytes(n))
}

// RandomHex returns n random bytes from a cryptographically secure source
// as hex, e.g. for ids and file names.
func RandomHex(n int) string {
	return hex.EncodeToString(randomBytes(n))
}

// randomBytes reads n secure random bytes. crypto/rand.Read never fails
// (Go 1.24+): it crashes the program rather than return weak randomness.
func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// HashHex is the SHA-256 of s as hex. It suits values that are already
// random (tokens): it is not a password hash.
func HashHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
