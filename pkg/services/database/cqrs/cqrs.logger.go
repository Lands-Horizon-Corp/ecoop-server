package cqrs

import (
	"context"
	"errors"
	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
)

// CQRS logs are for monitoring: a short, stable message that can be alerted on, with the detail in
// structured fields (component, channel, entity, then whatever the call site adds). Without an injected
// logger every helper is a no-op.

func (c *CQRSService[TData, TResponse, TRequest, TID]) fields(kv []any) []any {
	return append([]any{"component", "cqrs", "channel", string(c.Channel), "entity", c.entity}, kv...)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) emit(name string, write func(logger.LoggerLevel)) {
	if c.Log != nil {
		c.Log.Emit(name, write)
	}
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) info(_ context.Context, msg string, kv ...any) {
	c.emit("cqrs.info", func(l logger.LoggerLevel) {
		l.Info(msg, c.fields(kv)...)
	})
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) error(_ context.Context, err error, msg string, kv ...any) {
	if err == nil {
		err = errors.New(msg)
	}
	c.emit("cqrs.error", func(l logger.LoggerLevel) {
		l.Error(sqlsvc.Redact(err), msg, c.fields(kv)...)
	})
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) warn(_ context.Context, msg string, kv ...any) {
	c.emit("cqrs.warn", func(l logger.LoggerLevel) {
		l.Warn(msg, c.fields(kv)...)
	})
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) success(_ context.Context, msg string, kv ...any) {
	c.emit("cqrs.success", func(l logger.LoggerLevel) {
		l.Info(msg, c.fields(append([]any{"status", "success"}, kv...))...)
	})
}
