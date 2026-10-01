package logger

import (
	"fmt"

	"go.uber.org/zap"
)

func toFields(kv ...any) []zap.Field {
	var fields []zap.Field
	for i := 0; i < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			k = fmt.Sprintf("key_%d", i)
		}
		var v any
		if i+1 < len(kv) {
			v = kv[i+1]
		}
		fields = append(fields, zap.Any(k, v))
	}
	return fields
}
