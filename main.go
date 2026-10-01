package main

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
)

func main() {
	l := logger.NewLogContextService("sample", "", "")

	l.Start(context.Background())

	ctx, a, span := l.Trace("sample")
	a.Debug(ctx, "This is a debug message")
	span.End()

	l.Stop(context.Background())
}
