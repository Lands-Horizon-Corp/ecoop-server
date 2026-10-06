package regressions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broker"
)

// The broker tests run against the Kafka in docker-compose.yml (`make test-up`). KAFKA_TEST_BROKERS
// overrides the address. Each test uses its own topic and consumer group, so tests never see each
// other's messages.

func kafkaBrokers(t *testing.T) []string {
	t.Helper()
	addr := envOr("KAFKA_TEST_BROKERS", "localhost:9092")
	requireReachable(t, addr)
	return []string{addr}
}

func uniqueName(prefix string) string { return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()) }

func newBroker(t *testing.T, group string, log *recordingLog) broker.BatchBrokerServices {
	t.Helper()
	return newBrokerWith(t, group, broker.Options{}, log)
}

func newBrokerWith(t *testing.T, group string, opts broker.Options, log *recordingLog) broker.BatchBrokerServices {
	t.Helper()
	var b broker.BatchBrokerServices
	if log != nil {
		b = broker.NewBrokerService(kafkaBrokers(t), group, opts, log)
	} else {
		b = broker.NewBrokerService(kafkaBrokers(t), group, opts, nil)
	}
	if err := b.Run(bg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(bg) })
	return b
}

type received struct{ key, value string }

// collect subscribes in the background and returns a channel of what the handler saw plus a stop func
// that ends the subscription and returns what Subscribe returned.
func collect(t *testing.T, b broker.MessageBrokerServices, topic string, handler func(key, value []byte) error) (<-chan received, func() error) {
	t.Helper()
	got := make(chan received, 256)
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() {
		done <- b.Subscribe(ctx, topic, func(key, value []byte) error {
			if handler != nil {
				if err := handler(key, value); err != nil {
					return err
				}
			}
			got <- received{string(key), string(value)}
			return nil
		})
	}()
	var once sync.Once
	var result error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(15 * time.Second):
				result = errors.New("Subscribe did not return after its context ended")
			}
		})
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return got, stop
}

func next(t *testing.T, ch <-chan received) received {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(20 * time.Second):
		t.Fatal("no message arrived")
		return received{}
	}
}

func TestBroker_PublishedMessagesReachASubscriberWithTheirKeys(t *testing.T) {
	topic, group := uniqueName("rt"), uniqueName("g")
	b := newBroker(t, group, nil)

	// Published before anyone subscribes: a new group starts from the beginning, so nothing is missed.
	for i := range 3 {
		if err := b.Publish(bg, topic, fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	got, stop := collect(t, b, topic, nil)

	seen := map[string]string{}
	for range 3 {
		r := next(t, got)
		seen[r.key] = r.value
	}
	for i := range 3 {
		if seen[fmt.Sprintf("k%d", i)] != fmt.Sprintf("v%d", i) {
			t.Fatalf("received %v; want every key with its own value", seen)
		}
	}
	if err := stop(); err != nil {
		t.Fatalf("Subscribe ended with %v after its context was cancelled; want nil", err)
	}
}

func TestBroker_MessagesWithOneKeyArriveInOrder(t *testing.T) {
	topic, group := uniqueName("order"), uniqueName("g")
	b := newBroker(t, group, nil)
	got, _ := collect(t, b, topic, nil)

	const n = 30
	for i := range n {
		if err := b.Publish(bg, topic, []byte("account-1"), fmt.Appendf(nil, "%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		if r := next(t, got); r.value != fmt.Sprintf("%03d", i) {
			t.Fatalf("message %d arrived as %q; same-key messages must stay in order", i, r.value)
		}
	}
}

// At-least-once: a failing handler stops the subscription without committing the failed record, and the
// next subscriber in the group gets that record again, and everything after it.
func TestBroker_AFailedMessageIsRedeliveredToTheNextSubscriber(t *testing.T) {
	topic, group := uniqueName("redeliver"), uniqueName("g")
	b := newBroker(t, group, nil)
	for _, v := range []string{"m1", "m2", "m3"} {
		if err := b.Publish(bg, topic, []byte("k"), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	boom := errors.New("downstream is down")
	first, stop := collect(t, b, topic, func(_, value []byte) error {
		if string(value) == "m2" {
			return boom
		}
		return nil
	})
	if r := next(t, first); r.value != "m1" {
		t.Fatalf("first message = %q; want m1", r.value)
	}
	err := stop()
	if !errors.Is(err, boom) {
		t.Fatalf("Subscribe returned %v; want the handler's error", err)
	}

	second, _ := collect(t, b, topic, nil)
	for _, want := range []string{"m2", "m3"} {
		if r := next(t, second); r.value != want {
			t.Fatalf("after the failure got %q; want %s: m1 was committed, m2 was not", r.value, want)
		}
	}
}

func TestBroker_WithoutRunPublishFailsAndStopIsSafe(t *testing.T) {
	b := broker.NewBrokerService(kafkaBrokers(t), "g", broker.Options{}, nil)
	if err := b.Publish(bg, "t", nil, []byte("x")); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("Publish before Run = %v; want ErrNotRunning", err)
	}
	if err := b.Stop(bg); err != nil {
		t.Fatalf("Stop before Run = %v; want nil", err)
	}

	if err := b.Run(bg); err != nil {
		t.Fatal(err)
	}
	if err := b.Run(bg); err != nil {
		t.Fatalf("a second Run = %v; want a no-op", err)
	}
	if err := b.Stop(bg); err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(bg); err != nil {
		t.Fatalf("a second Stop = %v; want a no-op", err)
	}
	if err := b.Publish(bg, "t", nil, []byte("x")); !errors.Is(err, broker.ErrNotRunning) {
		t.Fatalf("Publish after Stop = %v; want ErrNotRunning", err)
	}
}

func TestBroker_ConfigurationErrorsAreExplicit(t *testing.T) {
	if err := broker.NewBrokerService(nil, "g", broker.Options{}, nil).Run(bg); !errors.Is(err, broker.ErrNoBrokers) {
		t.Fatalf("Run without brokers = %v; want ErrNoBrokers", err)
	}
	b := broker.NewBrokerService(kafkaBrokers(t), "", broker.Options{}, nil)
	if err := b.Subscribe(bg, "t", func(_, _ []byte) error { return nil }); !errors.Is(err, broker.ErrNoGroup) {
		t.Fatalf("Subscribe without a group = %v; want ErrNoGroup", err)
	}
}

func TestBroker_RunFailsFastWhenKafkaIsUnreachable(t *testing.T) {
	log := &recordingLog{Context: bg}
	b := broker.NewBrokerService([]string{closedAddr(t)}, "g", broker.Options{}, log)

	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	err := b.Run(ctx)
	if err == nil {
		_ = b.Stop(bg)
		t.Fatal("Run succeeded against a closed port")
	}
	e := log.await(t, "the failed start", withMsg("broker.run failed"))
	if e.level != "error" || e.err == nil || e.fields["component"] != "broker" {
		t.Errorf("line = %+v", e)
	}
}

func TestBroker_LogsReadinessAndNeverLogsMessageContents(t *testing.T) {
	topic, group := uniqueName("log"), uniqueName("g")
	log := &recordingLog{Context: bg}
	b := newBroker(t, group, log)
	log.await(t, "readiness", withMsg("broker ready"))

	const secret = "secret@example.com"
	if err := b.Publish(bg, topic, []byte("k"), []byte(secret)); err != nil {
		t.Fatal(err)
	}
	got, _ := collect(t, b, topic, nil)
	next(t, got)
	log.await(t, "the subscription", withMsg("broker subscribed"))

	pub := log.await(t, "the publish", withMsg("broker.publish done"))
	if pub.level != "debug" || pub.fields["topic"] != topic {
		t.Errorf("publish line = %+v; want a debug line naming the topic", pub)
	}
	for _, e := range log.snapshot() {
		if text := fmt.Sprintf("%v %v %v", e.msg, e.err, e.fields); strings.Contains(text, secret) {
			t.Fatalf("a message value reached the log: %+v", e)
		}
	}
}
