package utils

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
)

func DecodeQueryParam[T any](raw string) (T, error) {
	var value T
	unescaped, err := url.QueryUnescape(raw)
	if err != nil {
		return value, fmt.Errorf("unescaping failed: %w", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(unescaped)
	if err != nil {
		return value, fmt.Errorf("base64 decoding failed: %w", err)
	}
	if err := json.Unmarshal(decoded, &value); err != nil {
		return value, fmt.Errorf("JSON unmarshalling failed: %w", err)
	}
	return value, nil
}

func EncodeQueryParam[T any](v T) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("JSON marshalling failed: %w", err)
	}
	return url.QueryEscape(base64.StdEncoding.EncodeToString(raw)), nil
}
