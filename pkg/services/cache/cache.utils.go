package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

func globEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '*', '?', '[', ']', '\\':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func encode(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("cache: encode value: %w", err)
		}
		return data, nil
	}
}

func deleteKeys(ctx context.Context, client *redis.Client, keys []string) error {
	for i := 0; i < len(keys); i += deleteChunk {
		end := min(i+deleteChunk, len(keys))
		if err := client.Del(ctx, keys[i:end]...).Err(); err != nil {
			return fmt.Errorf("cache: delete keys: %w", err)
		}
	}
	return nil
}
