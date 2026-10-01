package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	l := logger.NewLogContextService("sample", "", "")

	l.Start(context.Background())

	ctx, lvl, span := l.Trace("demo.checkout")
	lvl.Info(ctx, "checkout started")

	// dbExec already logged the error once at the source; nothing here
	// re-logs it, so the span's error status set there isn't overwritten.
	_ = apiHandler(ctx, lvl)

	span.End()

	l.Stop(context.Background())
}

func apiHandler(ctx context.Context, lvl logger.LoggerLevel) error {
	return authCheck(ctx, lvl)
}

func authCheck(ctx context.Context, lvl logger.LoggerLevel) error {
	return orderValidate(ctx, lvl)
}

func orderValidate(ctx context.Context, lvl logger.LoggerLevel) error {
	return inventoryReserve(ctx, lvl)
}

func inventoryReserve(ctx context.Context, lvl logger.LoggerLevel) error {
	return paymentCharge(ctx, lvl)
}

func paymentCharge(ctx context.Context, lvl logger.LoggerLevel) error {
	return gatewayCall(ctx, lvl)
}

func gatewayCall(ctx context.Context, lvl logger.LoggerLevel) error {
	return ledgerWrite(ctx, lvl)
}

func ledgerWrite(ctx context.Context, lvl logger.LoggerLevel) error {
	return dbExec(ctx, lvl)
}

// dbExec is where the failure actually happens, so it's the only place
// that logs the error (and records the exception event on the span).
// Everything above just wraps and propagates it up.
func dbExec(ctx context.Context, lvl logger.LoggerLevel) error {
	err := errors.New("duplicate key value violates unique constraint")
	lvl.Error(ctx, err, "db exec failed")
	return fmt.Errorf("db exec: %w", err)
}
