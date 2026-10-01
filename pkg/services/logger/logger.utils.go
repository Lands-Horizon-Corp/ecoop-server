package logger

import (
	"context"
	"strings"

	"go.uber.org/zap"
)

var sensitiveKeys = map[string]struct{}{
	"password": {}, "token": {}, "authorization": {}, "secret": {},
	"email": {}, "phone": {}, "otp": {}, "pin": {},
}

func toFields(ctx context.Context, kv []any) []zap.Field {
	fs := make([]zap.Field, 0, len(kv)/2+1)
	if ctx != nil {
		fs = append(fs, zap.Any("ctx", ctx))
	}
	for i := 0; i < len(kv); i += 2 {
		key, ok := kv[i].(string)
		if !ok {
			fs = append(fs, zap.Any("!BADKEY", kv[i]))
			i--
			continue
		}
		if i+1 >= len(kv) {
			fs = append(fs, zap.String("!BADKEY", key))
			break
		}
		val := kv[i+1]
		if _, secret := sensitiveKeys[strings.ToLower(key)]; secret {
			val = "[REDACTED]"
		}
		fs = append(fs, zap.Any(key, val))
	}
	return fs
}
