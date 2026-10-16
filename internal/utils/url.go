package utils

import "net/url"

// WithQueryDefault is rawURL with the query parameter key set to value, unless
// it already has one. A rawURL that cannot be parsed, or has no scheme (a
// key=value DSN, say), is returned unchanged.
func WithQueryDefault(rawURL, key, value string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" {
		return rawURL
	}
	q := u.Query()
	if q.Has(key) {
		return rawURL
	}
	q.Set(key, value)
	u.RawQuery = q.Encode()
	return u.String()
}
