package utils

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/bytedance/sonic"
)

var ErrNilValue = errors.New("value is nil")

func Encode(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case bool:
		return strconv.FormatBool(v), nil
	}
	if IsNil(value) {
		return "", ErrNilValue
	}
	b, err := sonic.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encoding value: %w", err)
	}
	return string(b), nil
}

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
