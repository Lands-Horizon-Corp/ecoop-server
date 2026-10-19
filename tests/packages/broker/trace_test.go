//go:build integration

package broker_test

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability/otelobs"
)

// tracedBroker is a running broker whose observer exports spans to memory.
func tracedBroker(t testing.TB, group string) (broker.IBrokerService, *tracetest.InMemoryExporter, *otelobs.Backend, observability.IObserver) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	spans := tracetest.NewInMemoryExporter()
	be, err := otelobs.Open(otelobs.Config{Sampler: "always_on"}, otelobs.WithSpanExporter(spans), otelobs.WithMetricReader(sdkmetric.NewManualReader()))
	if err != nil {
		t.Fatalf("otelobs.Open: %v", err)
	}
	svc := observability.New(observability.Resource{ServiceName: "ecoop-test"}, slog.New(slog.DiscardHandler), observability.WithBackend(be))
	b := broker.NewBrokerService([]string{addr}, group, nil, broker.WithObserver(svc))
	if err := b.Run(ctxT(t)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()); _ = svc.Stop(context.Background()) })
	return b, spans, be, svc
}

// The consumer's span is a child of the publisher's, across Kafka: one trace for
// a message from the moment it is published to the moment it is handled.
func TestTrace_ConsumerContinuesThePublishersTrace(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b, spans, be, svc := tracedBroker(t, group)

	var (
		mu       sync.Mutex
		inHandle string
	)
	got := make(chan struct{}, 1)
	if err := b.Subscribe(ctxT(t), topic, func(ctx context.Context, _, _ []byte) error {
		mu.Lock()
		// The handler's own context carries the trace, so anything it calls joins it.
		_, op := svc.Begin(ctx, observability.ComponentSQL, "ping")
		op.End(nil)
		inHandle = "ran"
		mu.Unlock()
		got <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	pctx, root := svc.Begin(ctxT(t), observability.ComponentAPI, "/orders")
	if err := b.Publish(pctx, topic, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	root.End(nil)
	select {
	case <-got:
	case <-time.After(45 * time.Second):
		t.Fatal("the message was not delivered")
	}
	time.Sleep(200 * time.Millisecond)
	_ = be.ForceFlush(context.Background())

	byName := map[string]tracetest.SpanStub{}
	for _, s := range spans.GetSpans() {
		byName[s.Name] = s
	}
	pub, handle, sql := byName["broker publish"], byName["broker handle"], byName["sql ping"]
	if pub.Name == "" || handle.Name == "" || sql.Name == "" {
		t.Fatalf("missing spans: have %v", func() []string {
			var n []string
			for k := range byName {
				n = append(n, k)
			}
			return n
		}())
	}
	if inHandle != "ran" {
		t.Fatal("the handler did not run")
	}
	traceID := pub.SpanContext.TraceID()
	if handle.SpanContext.TraceID() != traceID || sql.SpanContext.TraceID() != traceID {
		t.Errorf("the trace broke across Kafka: publish %s, handle %s, handler's work %s",
			traceID, handle.SpanContext.TraceID(), sql.SpanContext.TraceID())
	}
	if handle.Parent.SpanID() != pub.SpanContext.SpanID() {
		t.Errorf("the handle span's parent is %s, want the publish span %s", handle.Parent.SpanID(), pub.SpanContext.SpanID())
	}
	if sql.Parent.SpanID() != handle.SpanContext.SpanID() {
		t.Errorf("the handler's work is not a child of the handle span")
	}
}

// A message published with no trace (an old producer, Debezium) is still handled,
// as the root of its own trace.
func TestTrace_MessageWithoutTraceIsStillHandled(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b, spans, be, _ := tracedBroker(t, group)
	var in inbox
	if err := b.Subscribe(ctxT(t), topic, in.handle); err != nil {
		t.Fatal(err)
	}
	// An uninstrumented publisher: a second broker with no observer at all.
	plain := service(t, group+"-plain")
	publish(t, plain, topic, 1, "k")
	in.wait(t, 1)
	time.Sleep(200 * time.Millisecond)
	_ = be.ForceFlush(context.Background())
	for _, s := range spans.GetSpans() {
		if s.Name == "broker handle" && s.Parent.IsValid() {
			t.Errorf("a handle span has a parent although the message carried no trace: %v", s.Parent)
		}
	}
}

// A hostile traceparent on a record (a poisoned producer) is ignored, and the
// message is handled normally.
func TestTrace_HostileTraceparentOnARecordIsIgnored(t *testing.T) {
	t.Parallel()
	topic, group := names()
	b, spans, be, _ := tracedBroker(t, group)
	var in inbox
	if err := b.Subscribe(ctxT(t), topic, in.handle); err != nil {
		t.Fatal(err)
	}
	// A producer that is not ours writes whatever it likes in the headers.
	raw, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)
	hostile := []string{"garbage", strings.Repeat("a", 200000), "00-0-0-0", "\x00\x01", "ff-" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 16) + "-01"}
	for i, v := range hostile {
		rec := &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte(strconv.Itoa(i)), Headers: []kgo.RecordHeader{
			{Key: "traceparent", Value: []byte(v)}, {Key: "tracestate", Value: []byte(v)}, {Key: "baggage", Value: []byte(v)},
		}}
		if err := raw.ProduceSync(ctxT(t), rec).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	in.wait(t, len(hostile)) // every message is still handled
	time.Sleep(200 * time.Millisecond)
	_ = be.ForceFlush(context.Background())
	for _, s := range spans.GetSpans() {
		if s.Name == "broker handle" && s.Parent.IsValid() {
			t.Errorf("a hostile header produced a parent: %v", s.Parent)
		}
	}
}
