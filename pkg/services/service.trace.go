package services

import (
	"context"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type TraceService struct {
	tracer trace.Tracer
}

// Takes *Telemetry only so fx builds (and registers the global provider) first.
func NewTraceService(tel *Telemetry) *TraceService {
	return &TraceService{tracer: otel.Tracer(tel.ServiceName)}
}

// Start opens a span. Always `defer span.End()`.
// Name spans like "loan.approve" or "member.create".
func (t *TraceService) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

func (t *TraceService) RecordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// HTTPMiddleware starts a server span per request and extracts incoming
// traceparent headers. Adapt if the router is gin/echo (use otelgin/otelecho).
func (t *TraceService) HTTPMiddleware(next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "http.server")
}
