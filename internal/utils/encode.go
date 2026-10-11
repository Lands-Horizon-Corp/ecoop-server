package utils

import (
	"errors"
	"fmt"
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
