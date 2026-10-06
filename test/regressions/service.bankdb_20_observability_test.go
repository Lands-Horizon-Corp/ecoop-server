package regressions

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
)

// 20 Observability: every failure path of the runner and the pools is visible to an operator with the
// fields they filter on (event id, topic, attempt, cause), and failed operations mark their span.

func accountMsg(t *testing.T, eventID string, a bdAccount) broker.Message {
	t.Helper()
	v, err := json.Marshal(cqrs.CQRSQueuePayload[bdAccount]{EventID: eventID, ChangeType: cqrs.ChangeTypeCreated, Payload: a})
	must(t, err)
	return broker.Message{Key: []byte(eventID), Value: v}
}

func TestBankDBObservability_RunnerFailurePathsAreLoggedWithContext(t *testing.T) {
	log := &recordingLog{Context: bg}
	bb := newBatchBroker()
	b := newBDBank(t, bdOpts{broker: bb, log: log})
	now := time.Now()

	t.Run("malformed message is logged and dead-lettered", func(t *testing.T) {
		must(t, bb.deliver(t, "bank_accounts", broker.Message{Key: []byte("k-bad"), Value: []byte("{oops"), Offset: 41}))
		bad := log.await(t, "unmarshal", withMsg("outbox payload unmarshal failed"))
		if bad.level != "error" || bad.fields["key"] != "k-bad" || bad.fields["offset"] != int64(41) {
			t.Errorf("unmarshal line = %+v", bad)
		}
		dl := log.await(t, "dead-letter", withMsg("outbox message dead-lettered"))
		if dl.fields["topic"] != "bank_accounts.dlq" || dl.fields["key"] != "k-bad" {
			t.Errorf("dead-letter line = %+v", dl)
		}
		if n := len(bb.publishedTo("bank_accounts.dlq")); n != 1 {
			t.Fatalf("%d messages on the dead-letter topic; want 1", n)
		}
	})

	t.Run("a rejected row is isolated, logged and dead-lettered", func(t *testing.T) {
		_, err := b.h.reader.Exec(`ALTER TABLE bank_accounts ADD CONSTRAINT no_poison CHECK (owner <> 'poison')`)
		must(t, err)
		must(t, bb.deliver(t, "bank_accounts",
			accountMsg(t, "e-ok", bdAccount{ID: "ok", Owner: "fine", Balance: 1, Version: 1, UpdatedAt: now}),
			accountMsg(t, "e-poison", bdAccount{ID: "p", Owner: "poison", Balance: 1, Version: 1, UpdatedAt: now})))
		log.await(t, "fallback", withMsg("outbox batch failed, retrying messages individually"))
		rej := log.await(t, "rejection", withMsg("outbox message rejected by read db"))
		if rej.fields["event_id"] != "e-poison" || rej.err == nil || !strings.Contains(rej.err.Error(), "23514") {
			t.Errorf("rejection line = %+v", rej)
		}
		if n := count(t, b.h.reader, `SELECT count(*) FROM bank_accounts WHERE id = 'ok'`); n != 1 {
			t.Fatal("the valid row was not applied")
		}
	})

	t.Run("a transient outage is retried and logged per attempt", func(t *testing.T) {
		readerDB := strings.TrimPrefix(mustURLPath(t, b.h.readerDSN), "/")
		admin := openInspect(t, envOr("SQL_TEST_DSN", defaultPostgresDSN))
		_, err := admin.Exec(fmt.Sprintf(`ALTER DATABASE %s ALLOW_CONNECTIONS false`, readerDB))
		must(t, err)
		_, err = admin.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, readerDB)
		must(t, err)
		done := make(chan error, 1)
		go func() {
			done <- bb.deliver(t, "bank_accounts", accountMsg(t, "e-late", bdAccount{ID: "late", Owner: "late", Balance: 1, Version: 1, UpdatedAt: now}))
		}()
		retry := log.await(t, "retry", withMsg("outbox batch failed transiently, retrying"))
		if retry.level != "warn" || retry.fields["attempt"] == nil {
			t.Errorf("retry line = %+v", retry)
		}
		select {
		case err := <-done:
			t.Fatalf("the batch was acknowledged (%v) while the reader was down", err)
		case <-time.After(300 * time.Millisecond):
		}
		_, err = admin.Exec(fmt.Sprintf(`ALTER DATABASE %s ALLOW_CONNECTIONS true`, readerDB))
		must(t, err)
		select {
		case err := <-done:
			must(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("the batch was never applied after the reader came back")
		}
		b.h.reader = openInspect(t, b.h.readerDSN)
		if n := count(t, b.h.reader, `SELECT count(*) FROM bank_accounts WHERE id = 'late'`); n != 1 {
			t.Fatal("the retried change is missing")
		}
	})
}

func TestBankDBObservability_FailedPoolOperationsMarkTheirSpan(t *testing.T) {
	log := &recordingLog{Context: bg}
	h := newDBHarness(t)
	writeBankMigration(t)
	svc := newBDService(t, h, bdOpts{maxOpen: 2, app: "spans", log: log,
		route: func(string) string {
			return fmt.Sprintf("postgres://x:y@%s/none?sslmode=disable&connect_timeout=2", closedAddr(t))
		}})
	if err := svc.Start(bg); err == nil {
		t.Fatal("Start succeeded against a closed port")
	}
	e := log.await(t, "failed span", func(e logEvent) bool { return e.span == "sql.run" && e.level == "error" })
	if e.err == nil {
		t.Fatalf("failed span line has no error: %+v", e)
	}
	requireKind(t, e.err, database.ErrUnavailable)
}
