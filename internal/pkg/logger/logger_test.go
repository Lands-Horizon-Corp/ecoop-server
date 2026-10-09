package logger

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newStarted returns a started service whose spans go to an in-memory recorder.
// Exporters dial lazily, so no collector is needed.
func newStarted(t *testing.T) (*loggerServiceImpl, *tracetest.SpanRecorder) {
	t.Helper()
	l := NewLoggerService("test-svc", "debug", "json").(*loggerServiceImpl)
	if err := l.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_ = l.Stop(ctx) // no collector in tests; export errors on shutdown are expected
	})
	rec := tracetest.NewSpanRecorder()
	l.state.tracer = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test")
	return l, rec
}

func TestInjectBeforeStart(t *testing.T) {
	l := NewLoggerService("svc", "", "")
	checks := map[string]error{
		"InjectCache":      l.InjectCache(nil),
		"InjectHertz":      l.InjectHertz(nil),
		"InjectHTTPClient": l.InjectHTTPClient(&http.Client{}),
		"Stop":             l.Stop(context.Background()),
		"Job":              l.Job(context.Background(), "j", func(LogContextService, LoggerLevel) error { return nil }),
	}
	checks["InjectDB"] = l.InjectDB(nil, "writer")
	if _, err := l.InjectBrokerSyncProducer(nil, nil); err != nil {
		checks["InjectBrokerSyncProducer"] = err
	}
	for name, err := range checks {
		if !errors.Is(err, ErrNotStarted) {
			t.Errorf("%s: got %v, want ErrNotStarted", name, err)
		}
	}
}

func TestStartValidation(t *testing.T) {
	if err := NewLoggerService("svc", "loud", "").Start(context.Background()); err == nil || !strings.Contains(err.Error(), "invalid log level") {
		t.Errorf("bad level: got %v", err)
	}
	if err := NewLoggerService("svc", "", "xml").Start(context.Background()); err == nil || !strings.Contains(err.Error(), "invalid log format") {
		t.Errorf("bad format: got %v", err)
	}
	if err := NewLoggerService("", "", "").Start(context.Background()); err == nil {
		t.Error("empty name: want error")
	}

	l, _ := newStarted(t)
	if err := l.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("second Start: got %v, want ErrAlreadyStarted", err)
	}
}

func TestInjectNilTargets(t *testing.T) {
	l, _ := newStarted(t)
	checks := map[string]error{
		"InjectCache":                   l.InjectCache(nil),
		"InjectHertz":                   l.InjectHertz(nil),
		"InjectHertzClient":             l.InjectHertzClient(nil),
		"InjectHTTPClient":              l.InjectHTTPClient(nil),
		"InjectPusher":                  l.InjectPusher(nil),
		"InjectBrokerConfig":            l.InjectBrokerConfig(nil),
		"Job":                           l.Job(context.Background(), "j", nil),
		"InjectBrokerConsumerGroup":     second(l.InjectBrokerConsumerGroup(nil)),
		"InjectBrokerConsumer":          second(l.InjectBrokerConsumer(nil)),
		"InjectBrokerPartitionConsumer": second(l.InjectBrokerPartitionConsumer(nil)),
		"InjectBrokerAsyncProducer":     second(l.InjectBrokerAsyncProducer(nil, nil)),
		"InjectDB":                      l.InjectDB(nil, "writer"),
	}
	for name, err := range checks {
		if !errors.Is(err, ErrNilTarget) {
			t.Errorf("%s: got %v, want ErrNilTarget", name, err)
		}
	}
	if err := l.InjectBrokerConfig(sarama.NewConfig()); err != nil {
		t.Errorf("InjectBrokerConfig: %v", err)
	}
}

func second[T any](_ T, err error) error { return err }

func TestHertzSpans(t *testing.T) {
	l, rec := newStarted(t)
	h := server.Default()
	if err := l.InjectHertz(h); err != nil {
		t.Fatal(err)
	}
	h.GET("/users/:id", func(ctx context.Context, c *app.RequestContext) { c.String(200, "ok") })
	h.GET("/boom", func(ctx context.Context, c *app.RequestContext) { panic("boom") })

	ut.PerformRequest(h.Engine, "GET", "/users/7", nil,
		ut.Header{Key: "X-Forwarded-Proto", Value: "https"},
		ut.Header{Key: "traceparent", Value: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"})
	ut.PerformRequest(h.Engine, "GET", "/boom", nil)

	spans := rec.Ended()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
	ok, boom := spans[0], spans[1]
	if ok.Name() != "GET /users/:id" {
		t.Errorf("name = %q", ok.Name())
	}
	if ok.Parent().TraceID().String() != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("incoming trace not continued: parent %s", ok.Parent().TraceID())
	}
	if got := attr(ok, "url.scheme"); got != "https" {
		t.Errorf("url.scheme = %q, want https", got)
	}
	if boom.Status().Code != codes.Error || len(boom.Events()) == 0 {
		t.Errorf("panic not recorded: status %v, events %d", boom.Status(), len(boom.Events()))
	}
}

func TestLogLevelsBecomeSpanEvents(t *testing.T) {
	l, rec := newStarted(t)
	_, log := l.NewContext(context.Background()).Trace("work")
	log.Info("step one", "n", 1)
	log.Event("checkpoint", "ok", true)
	log.Success("done")
	log.Span().End()

	s := rec.Ended()[0]
	if s.Status().Code != codes.Ok {
		t.Errorf("Success should set status OK, got %v", s.Status())
	}
	var names []string
	for _, e := range s.Events() {
		names = append(names, e.Name)
	}
	if strings.Join(names, ",") != "step one,checkpoint,done" {
		t.Errorf("events = %v", names)
	}

	_, log = l.NewContext(context.Background()).Trace("broken")
	log.Fatal(errors.New("disk full"), "cannot write")
	log.Span().End()
	if f := rec.Ended()[1]; f.Status().Code != codes.Error {
		t.Errorf("Fatal should set status Error, got %v", f.Status())
	}
}

func TestJob(t *testing.T) {
	l, rec := newStarted(t)

	if err := l.Job(context.Background(), "ok-job", func(LogContextService, LoggerLevel) error { return nil }); err != nil {
		t.Errorf("ok job: %v", err)
	}
	err := l.Job(context.Background(), "panic-job", func(LogContextService, LoggerLevel) error { panic("kaboom") })
	if err == nil || !strings.Contains(err.Error(), "kaboom") {
		t.Errorf("panic job: got %v, want error containing kaboom", err)
	}

	spans := rec.Ended()
	if len(spans) != 2 || spans[0].Status().Code != codes.Ok || spans[1].Status().Code != codes.Error {
		t.Errorf("unexpected job spans: %d", len(spans))
	}
}

func attr(s sdktrace.ReadOnlySpan, key string) string {
	for _, a := range s.Attributes() {
		if string(a.Key) == key {
			return a.Value.Emit()
		}
	}
	return ""
}
