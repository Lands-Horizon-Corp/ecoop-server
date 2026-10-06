package regressions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/uptrace/bun"
)

// CQRS monitoring: every line the service writes must carry a stable message plus the fields an operator
// filters on (channel, entity, event type, counts, the error), and a missing logger must change nothing.

type cqAccount struct {
	bun.BaseModel `bun:"table:cq_accounts"`
	ID            string `bun:"id,pk"`
	Name          string `bun:"name,notnull"`
}

type (
	cqResource struct{ ID string }
	cqRequest  struct{ Name string }
	cqService  = cqrs.CQRSServices[cqAccount, cqResource, cqRequest, string]
)

// cqSQL is a write database that only answers Ping, which is all Run asks of it.
type cqSQL struct{ sqlsvc.SQLServices }

func (cqSQL) Ping(context.Context) error { return nil }

type cqPagination struct {
	pagination.PaginationServices[cqAccount, string]
}

type cqBroker struct {
	handler chan func(key, value []byte) error
}

func (*cqBroker) Publish(context.Context, string, []byte, []byte) error { return nil }
func (b *cqBroker) Subscribe(ctx context.Context, _ string, h func(key, value []byte) error) error {
	b.handler <- h
	<-ctx.Done()
	return nil
}

type cqBroadcast struct{ err error }

func (b cqBroadcast) Broadcast([]broadcast.Channel, broadcast.Events, any) error { return b.err }

func cqNew(log *recordingLog, mutate func(*cqrs.CQRSService[cqAccount, cqResource, cqRequest, string])) cqService {
	c := cqrs.CQRSService[cqAccount, cqResource, cqRequest, string]{
		Channel:           "accounts",
		ToResource:        func(a *cqAccount) *cqResource { return &cqResource{ID: a.ID} },
		Created:           func(*cqAccount) broadcast.Events { return broadcast.Events{"account.created"} },
		WriteSQLService:   cqSQL{},
		PaginationService: cqPagination{},
		BatchSize:         3,
		FlushInterval:     30 * time.Millisecond,
	}
	if log != nil {
		c.Log = log
	}
	if mutate != nil {
		mutate(&c)
	}
	return cqrs.NewCQRS(c)
}

// cqRun starts the outbox runner against a fake broker and returns the handler it subscribed.
func cqRun(t *testing.T, svc cqService, broker *cqBroker) func(key, value []byte) error {
	t.Helper()
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case h := <-broker.handler:
		return h
	case err := <-done:
		t.Fatalf("Run returned before subscribing: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run never subscribed")
	}
	return nil
}

func TestCQRSLogging_EventFailuresCarryTheirContext(t *testing.T) {
	log := &recordingLog{Context: bg}
	dispatchErr, broadcastErr := errors.New("dispatch down"), errors.New("broadcast down")
	svc := cqNew(log, func(c *cqrs.CQRSService[cqAccount, cqResource, cqRequest, string]) {
		c.Dispatch = func(broadcast.Channel, broadcast.Events, *cqResource) error { return dispatchErr }
		c.BroadcastService = cqBroadcast{err: broadcastErr}
	})

	svc.OnCreated(bg, &cqAccount{ID: "a1", Name: "Ann"})

	for msg, want := range map[string]error{"event dispatch failed": dispatchErr, "event broadcast failed": broadcastErr} {
		e := log.await(t, msg, withMsg(msg))
		if e.level != "error" || e.span != "cqrs.error" || !errors.Is(e.err, want) {
			t.Errorf("%q = %+v; want an error line carrying %v", msg, e, want)
		}
		for field, v := range map[string]any{"component": "cqrs", "channel": "accounts", "entity": "regressions.cqAccount", "event_type": "created"} {
			if e.fields[field] != v {
				t.Errorf("%q field %s = %v; want %v", msg, field, e.fields[field], v)
			}
		}
	}
}

func TestCQRSLogging_AHandlerPanicIsLoggedNotLost(t *testing.T) {
	log := &recordingLog{Context: bg}
	svc := cqNew(log, func(c *cqrs.CQRSService[cqAccount, cqResource, cqRequest, string]) {
		c.ToResource = func(*cqAccount) *cqResource { panic("boom") }
	})

	svc.OnCreated(bg, &cqAccount{ID: "a1"})

	e := log.await(t, "the panic", withMsg("event handler panicked"))
	if e.level != "error" || e.err == nil || e.err.Error() != "panic: boom" || e.fields["event_type"] != "created" {
		t.Fatalf("panic line = %+v; want the recovered value as the error", e)
	}
}

func TestCQRSLogging_RunnerReportsStartupBadInputAndFailedBatches(t *testing.T) {
	log := &recordingLog{Context: bg}
	broker := &cqBroker{handler: make(chan func(key, value []byte) error, 1)}
	svc := cqNew(log, func(c *cqrs.CQRSService[cqAccount, cqResource, cqRequest, string]) {
		c.MessageBrokerService = broker // no read database: every batch must fail loudly
	})
	handle := cqRun(t, svc, broker)

	started := log.await(t, "startup", withMsg("outbox runner started"))
	if started.level != "info" || started.fields["batch_size"] != 3 || started.fields["flush_interval"] != "30ms" {
		t.Errorf("startup line = %+v", started)
	}

	if err := handle([]byte("k1"), []byte("{not json")); err != nil {
		t.Fatalf("a malformed payload must be logged and skipped, not returned: %v", err)
	}
	bad := log.await(t, "bad payload", withMsg("outbox payload unmarshal failed"))
	if bad.level != "error" || bad.err == nil || bad.fields["key"] != "k1" || bad.fields["bytes"] != len("{not json") {
		t.Errorf("unmarshal line = %+v; want the key and payload size", bad)
	}

	if err := handle(nil, []byte(`{"change_type":1,"payload":{"id":"a1","name":"Ann"}}`)); err != nil {
		t.Fatal(err)
	}
	warn := log.await(t, "missing event id", withMsg("outbox message has no event id and no key; synthesized one, check the producer"))
	if id, _ := warn.fields["synthesized_event_id"].(string); warn.level != "warn" || id == "" {
		t.Errorf("missing-id line = %+v; want the synthesized id", warn)
	}

	failed := log.await(t, "failed batch", withMsg("outbox batch failed"))
	if failed.level != "error" || !errors.Is(failed.err, cqrs.ErrReadDBNotInitialized) || failed.fields["batch_size"] != 1 {
		t.Errorf("batch line = %+v; want ErrReadDBNotInitialized and the batch size", failed)
	}
}

// The success line is what an operator graphs: how many messages arrived, how many were new, how long it took.
func TestCQRSLogging_SynchronizedBatchReportsCountsAndDuration(t *testing.T) {
	e := newSQLEnv(t)
	e.exec(`CREATE TABLE processed_events (event_id text PRIMARY KEY, channel text NOT NULL, created_at timestamptz NOT NULL DEFAULT now())`)
	e.exec(`CREATE TABLE cq_accounts (id text PRIMARY KEY, name text NOT NULL)`)
	read := sqlsvc.NewSQLService(e.dsn, 2, 5, nil, false, nil, nil, nil)
	if err := read.Run(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = read.Stop(bg) })

	log := &recordingLog{Context: bg}
	broker := &cqBroker{handler: make(chan func(key, value []byte) error, 1)}
	svc := cqNew(log, func(c *cqrs.CQRSService[cqAccount, cqResource, cqRequest, string]) {
		c.MessageBrokerService = broker
		c.ReadSQLService = read
	})
	handle := cqRun(t, svc, broker)

	// Three deliveries fill one batch; two share an event id, as a redelivery would.
	for _, msg := range []string{
		`{"event_id":"e1","change_type":1,"payload":{"id":"a1","name":"Ann"}}`,
		`{"event_id":"e2","change_type":1,"payload":{"id":"a2","name":"Bob"}}`,
		`{"event_id":"e1","change_type":1,"payload":{"id":"a1","name":"Ann"}}`,
	} {
		if err := handle(nil, []byte(msg)); err != nil {
			t.Fatal(err)
		}
	}

	ok := log.await(t, "the sync summary", withMsg("read db synchronized"))
	if ok.level != "info" || ok.span != "cqrs.success" || ok.fields["status"] != "success" ||
		ok.fields["received"] != 3 || ok.fields["applied"] != 2 {
		t.Fatalf("summary = %+v; want received=3 applied=2 status=success", ok)
	}
	if ms, isInt := ok.fields["duration_ms"].(int64); !isInt || ms < 0 {
		t.Errorf("duration_ms = %v; want a non-negative integer", ok.fields["duration_ms"])
	}
	if got := e.scanInt(`SELECT count(*) FROM cq_accounts`); got != 2 {
		t.Fatalf("%d rows reached the read database; want 2", got)
	}
}

func TestCQRSLogging_NoLoggerChangesNothing(t *testing.T) {
	dispatched := make(chan struct{})
	svc := cqNew(nil, func(c *cqrs.CQRSService[cqAccount, cqResource, cqRequest, string]) {
		c.Dispatch = func(broadcast.Channel, broadcast.Events, *cqResource) error {
			defer close(dispatched)
			return errors.New("dispatch down") // would be logged; with no logger it must be silent, not a panic
		}
	})

	svc.OnCreated(bg, &cqAccount{ID: "a1"})

	select {
	case <-dispatched:
	case <-time.After(3 * time.Second):
		t.Fatal("the event was never dispatched")
	}
	time.Sleep(50 * time.Millisecond) // give a faulty helper time to blow up the goroutine
}
