package regressions

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

// CDC end to end (`make test-cdc`): real Debezium change events, real Kafka, the outbox runner with
// at-least-once delivery, and a Postgres 16 reader. The reader must end up identical to the writer.

func TestCDC_SmokeWritesFlowThroughDebeziumToTheReader(t *testing.T) {
	b, _ := newCDCBank(t, bdOpts{})
	ctx := withDeadline(t, 60*time.Second)
	closed := time.Now().UTC().Truncate(time.Microsecond)
	_, err := b.accounts.Create(ctx, bdAccount{ID: "a", Owner: "Ann", Balance: 1000,
		Profile: map[string]any{"tier": "gold", "tags": []any{"vip"}}, Signature: []byte{0, 1, 0xfe, 0xff}})
	must(t, err)
	_, err = b.accounts.Create(ctx, bdAccount{ID: "b", Owner: "Bo", Balance: 0})
	must(t, err)
	_, err = b.accounts.Create(ctx, bdAccount{ID: "c", Owner: "Cy", Balance: 5, ClosedAt: &closed}) // a closed account stays untouched
	must(t, err)
	must(t, second3(b.Transfer(ctx, transferReq{Key: "k1", From: "a", To: "b", Amount: 250})))

	awaitMirror(t, b) // every column of every table, including jsonb, bytea, enum and timestamps

	// Updates and deletes stream too.
	must(t, second3(b.Transfer(ctx, transferReq{Key: "k2", From: "b", To: "a", Amount: 50})))
	_, err = b.accounts.Create(ctx, bdAccount{ID: "temp", Owner: "Tmp", Balance: 1})
	must(t, err)
	awaitMirror(t, b)
	must(t, b.accounts.DeleteByID(ctx, "temp"))
	awaitMirror(t, b)

	// Every applied change came from Debezium (its event ids derive from the WAL position).
	if n := count(t, b.h.reader, `SELECT count(*) FROM processed_events WHERE event_id NOT LIKE 'dbz:%'`); n != 0 {
		t.Fatalf("%d processed events did not come from Debezium", n)
	}
	if n := count(t, b.h.reader, `SELECT count(*) FROM processed_events WHERE event_id LIKE 'dbz:public.bank_accounts:%:d:%'`); n != 1 {
		t.Fatalf("delete events applied = %d; want 1", n)
	}

	got, err := b.accounts.GetByID(ctx, "a") // served by pagination from the reader
	if err != nil || got.Balance != 800 || got.Profile["tier"] != "gold" || string(got.Signature) != string([]byte{0, 1, 0xfe, 0xff}) {
		t.Fatalf("reader account a = %+v, %v", got, err)
	}
}

func TestCDC_HappySnapshotDeliversRowsWrittenBeforeTheConnector(t *testing.T) {
	// Rows that exist before Debezium starts arrive as snapshot reads (op "r").
	s := &cdcStream{prefix: fmt.Sprintf("t%d", time.Now().UnixNano()), brokers: strings.Split(envOr("CDC_KAFKA_BROKERS", "localhost:9094"), ",")}
	s.kafka = s.newBroker(t, s.prefix+"-runner")
	b := newBDBank(t, bdOpts{target: "pg16", broker: s.kafka, channelPrefix: s.prefix + ".public."})
	ctx := withDeadline(t, 60*time.Second)
	for i := range 20 {
		_, err := b.accounts.Create(ctx, bdAccount{ID: fmt.Sprintf("pre%02d", i), Owner: fmt.Sprintf("o%d", i), Balance: int64(i)})
		must(t, err)
	}
	s.registerConnector(t, b)
	awaitMirror(t, b)
}

func TestCDC_SadRunnerStoppedMidStreamLosesAndDuplicatesNothing(t *testing.T) {
	b, _ := newCDCBank(t, bdOpts{})
	ctx := withDeadline(t, 120*time.Second)
	for _, id := range []string{"a", "b", "c"} {
		_, err := b.accounts.Create(ctx, bdAccount{ID: id, Owner: "o-" + id, Balance: 10_000})
		must(t, err)
	}
	ids := []string{"a", "b", "c"}
	for i := range 60 {
		if i == 20 {
			must(t, b.svc.Stop(bg)) // the app is killed while changes keep coming
			must(t, b.svc.Start(bg))
			b.bind()
		}
		if i == 40 {
			b.svc.Run(bg) // back up: resumes from the last committed offset
		}
		must(t, second3(b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%02d", i), From: ids[i%3], To: ids[(i+1)%3], Amount: 7})))
	}
	awaitMirror(t, b)
	if n := count(t, b.h.reader, `SELECT count(*) - count(DISTINCT event_id) FROM processed_events`); n != 0 {
		t.Fatalf("%d duplicate processed events", n)
	}
}

func TestCDC_SadRowTheReaderRejectsGoesToTheDeadLetterTopic(t *testing.T) {
	b, s := newCDCBank(t, bdOpts{})
	ctx := withDeadline(t, 60*time.Second)
	// The read model is stricter than the writer, e.g. after a reader-only migration.
	_, err := b.h.reader.Exec(`ALTER TABLE bank_accounts ADD CONSTRAINT no_poison CHECK (owner <> 'poison')`)
	must(t, err)
	dlq := s.collect(t, s.topic("bank_accounts")+".dlq")

	for _, a := range []bdAccount{{ID: "ok1", Owner: "fine", Balance: 1}, {ID: "bad", Owner: "poison", Balance: 1}, {ID: "ok2", Owner: "also fine", Balance: 2}} {
		_, err := b.accounts.Create(ctx, a)
		must(t, err)
	}
	b.awaitReader(`SELECT count(*) FROM bank_accounts WHERE id IN ('ok1', 'ok2')`, 2)

	deadline := time.Now().Add(30 * time.Second)
	for len(dlq()) == 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	msgs := dlq()
	if len(msgs) != 1 {
		t.Fatalf("dead-letter topic has %d messages; want exactly the poison row", len(msgs))
	}
	var rec struct {
		Channel, Error string
		Original       json.RawMessage
	}
	must(t, json.Unmarshal(msgs[0].Value, &rec))
	if !strings.Contains(string(rec.Original), `"poison"`) || !strings.Contains(rec.Error, "no_poison") {
		t.Fatalf("dead-letter record = %s; want the original row and the reason", msgs[0].Value)
	}
	if n := count(t, b.h.reader, `SELECT count(*) FROM bank_accounts WHERE id = 'bad'`); n != 0 {
		t.Fatal("the rejected row reached the reader")
	}
}

func TestCDC_HappyTwoInstancesInOneConsumerGroupConverge(t *testing.T) {
	b, s := newCDCBank(t, bdOpts{})
	ctx := withDeadline(t, 120*time.Second)
	// A second replica of the app, sharing the databases and the consumer group.
	replica := newBDService(t, b.h, bdOpts{maxOpen: 8, app: "replica", route: func(d string) string { return d },
		broker: s.newBroker(t, s.prefix+"-runner"), channelPrefix: s.prefix + ".public."})
	must(t, replica.Start(bg))
	t.Cleanup(func() { _ = replica.Stop(bg) })
	replica.Run(bg)

	ids := []string{"a", "b", "c", "d"}
	for _, id := range ids {
		_, err := b.accounts.Create(ctx, bdAccount{ID: id, Owner: "o-" + id, Balance: 5_000})
		must(t, err)
	}
	errs := runParallel(t, 80, func(ctx context.Context, i int) error {
		if i == 40 {
			_ = replica.Stop(bg) // one replica goes away mid-stream; the group rebalances
		}
		_, _, err := b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%02d", i), From: ids[i%4], To: ids[(i+1)%4], Amount: 3})
		return err
	})
	if len(errs) > 0 {
		t.Fatalf("%d transfers failed, first: %v", len(errs), errs[0])
	}
	awaitMirror(t, b)
}

// Without the live chain: a Debezium event with schemas enabled (the connector's other JsonConverter
// mode) decodes like the schemaless one.
func TestCDC_HappySchemaWrappedEnvelopeDecodes(t *testing.T) {
	b := newBDBank(t, bdOpts{})
	accounts := b.events["bank_accounts"]
	event := `{"schema":{"type":"struct"},"payload":{"before":null,"after":{"id":"x","owner":"Xi","currency":"PHP","balance":42,"version":1,
		"profile":"{\"tier\":\"gold\"}","signature":"AAH+/w==","closed_at":null,"updated_at":"2026-01-02T03:04:05.123456Z"},
		"source":{"db":"w","schema":"public","table":"bank_accounts","lsn":123,"txId":9},"op":"c","ts_ms":1}}`
	must(t, accounts([]byte(`{"id":"x"}`), []byte(event)))
	b.awaitReader(`SELECT count(*) FROM bank_accounts WHERE id = 'x' AND balance = 42 AND profile->>'tier' = 'gold'
		AND signature = '\x0001feff'::bytea AND updated_at = '2026-01-02T03:04:05.123456Z'`, 1)
	if n := count(t, b.h.reader, `SELECT count(*) FROM processed_events WHERE event_id LIKE 'dbz:public.bank_accounts:123:9:c:%'`); n != 1 {
		t.Fatal("the event id is not derived from the Debezium source position")
	}
}

// The reader becomes unreachable while changes stream in. With acknowledge-after-apply the runner
// retries until the reader is back and nothing is lost; acknowledging on receipt would drop them.
func TestCDC_SadReaderOutageIsRetriedNotLost(t *testing.T) {
	b, _ := newCDCBank(t, bdOpts{})
	ctx := withDeadline(t, 120*time.Second)
	_, err := b.accounts.Create(ctx, bdAccount{ID: "a", Owner: "Ann", Balance: 10_000})
	must(t, err)
	_, err = b.accounts.Create(ctx, bdAccount{ID: "b", Owner: "Bo", Balance: 0})
	must(t, err)
	awaitMirror(t, b)

	readerDB := strings.TrimPrefix(mustURLPath(t, b.h.readerDSN), "/")
	admin := openInspect(t, envOr("CDC_READ_DSN", defaultCDCReadDSN))
	outage := func(on bool) {
		_, err := admin.Exec(fmt.Sprintf(`ALTER DATABASE %s ALLOW_CONNECTIONS %t`, readerDB, !on))
		must(t, err)
		if on {
			_, err = admin.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, readerDB)
			must(t, err)
		}
	}
	outage(true)
	for i := range 10 { // these changes reach the runner while the reader is down
		must(t, second3(b.Transfer(ctx, transferReq{Key: fmt.Sprintf("k%d", i), From: "a", To: "b", Amount: 10})))
	}
	time.Sleep(time.Second) // several failed apply attempts
	outage(false)
	b.h.reader = openInspect(t, b.h.readerDSN) // the old inspection connection was terminated too
	awaitMirror(t, b)
}

func mustURLPath(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	must(t, err)
	return u.Path
}
