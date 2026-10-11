package utils

import (
	"crypto/rand"
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
