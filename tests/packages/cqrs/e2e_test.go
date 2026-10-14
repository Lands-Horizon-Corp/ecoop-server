//go:build integration

package cqrs_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cqrs"
)

// End to end: Postgres -> Debezium -> Kafka -> the real cdc service -> the CQRS
// service -> the read database. Each test registers its own connector (its own
// slot, publication and topic prefix) against its own database, so nothing here
// touches the stack's own connector or other tests. They skip when Kafka or
// Debezium is not running.

type stack struct {
	broker  string
	connect string
}

func connectURL() string {
	return "http://" + net.JoinHostPort(cfg("CDC_HOST", "127.0.0.1"), cfg("CDC_PORT", "8083"))
}

func skipUnlessDebezium(t testing.TB) stack {
	t.Helper()
	skipUnlessPostgres(t)
	s := stack{
		broker:  net.JoinHostPort(cfg("BROKER_HOST", "127.0.0.1"), cfg("BROKER_PORT", "9094")),
		connect: connectURL(),
	}
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(s.connect + "/connectors")
	if err != nil {
		t.Skipf("no Debezium at %s: %v", s.connect, err)
	}
	_ = resp.Body.Close()
	return s
}

// connector registers a Debezium connector for the test's write database and
// removes it, and its replication slot, when the test ends. It returns the topic
// prefix its topics use.
func (s stack) connector(t testing.TB, p *pair) (prefix string) {
	t.Helper()
	// The test tables are made FULL by newPair, as the app requires.
	return s.connectorWith(t, p, "")
}

// connectorWith is connector with extra JSON settings ("key": "value", ...)
// merged into the connector's configuration.
func (s stack) connectorWith(t testing.TB, p *pair, extra string) (prefix string) {
	t.Helper()
	if extra != "" {
		extra = "," + extra
	}
	n := seqNo.Add(1)
	prefix = fmt.Sprintf("e2e%d_%d", runID%1_000_000_000, n)
	name := prefix
	slot := prefix + "_slot"
	body := fmt.Sprintf(`{
		"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
		"plugin.name": "pgoutput",
		"database.hostname": "postgres", "database.port": "5432",
		"database.user": %q, "database.password": %q, "database.dbname": %q,
		"topic.prefix": %q,
		"table.include.list": "public.accounts",
		"slot.name": %q, "publication.name": %q,
		"publication.autocreate.mode": "filtered",
		"snapshot.mode": "initial",
		"tombstones.on.delete": "true",
		"decimal.handling.mode": "string",
		"key.converter": "org.apache.kafka.connect.json.JsonConverter",
		"key.converter.schemas.enable": "false",
		"value.converter": "org.apache.kafka.connect.json.JsonConverter",
		"value.converter.schemas.enable": "false",
		"topic.creation.default.replication.factor": "1",
		"topic.creation.default.partitions": "3"%s
	}`, p.writeSrv.user, p.writeSrv.password, p.writeDB, prefix, slot, slot, extra)
	connectorBodies.Store(prefix, body)
	s.putConnector(t, name, body)
	t.Cleanup(func() {
		req, _ := http.NewRequest(http.MethodDelete, s.connect+"/connectors/"+name, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
		// The slot is held until the connector's task has really gone; the
		// database cannot be dropped while it exists.
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			var active bool
			err := writeAdmin.QueryRow(`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&active)
			if errors.Is(err, sql.ErrNoRows) {
				return
			}
			if err == nil && !active {
				_, _ = writeAdmin.Exec(`SELECT pg_drop_replication_slot($1)`, slot)
				return
			}
			time.Sleep(300 * time.Millisecond)
		}
	})
	// Wait for the connector's task to run, so what the test writes is captured.
	waitRunning(t, s, name)
	return prefix
}

// connectorBodies holds the configuration each test connector was registered with.
var connectorBodies sync.Map

func (s stack) putConnector(t testing.TB, name, body string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, s.connect+"/connectors/"+name+"/config", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("registering the connector: %s: %s", resp.Status, out)
	}
}

// connectCall sends a REST request to Kafka Connect and returns the status code.
func (s stack) connectCall(method, path string) int {
	req, _ := http.NewRequest(method, s.connect+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// crashConnector removes the connector (and its task) without touching its
// replication slot or its stored offsets, then registers it again with the same
// configuration: what a crash and a restart of the connector look like.
func (s stack) crashConnector(t testing.TB, prefix string) {
	t.Helper()
	body, ok := connectorBodies.Load(prefix)
	if !ok {
		t.Fatalf("unknown connector %s", prefix)
	}
	s.connectCall(http.MethodDelete, "/connectors/"+prefix)
	deadline := time.Now().Add(30 * time.Second)
	for s.connectCall(http.MethodGet, "/connectors/"+prefix) == http.StatusOK {
		if time.Now().After(deadline) {
			t.Fatalf("connector %s would not go away", prefix)
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.putConnector(t, prefix, body.(string))
	waitRunning(t, s, prefix)
}

func waitRunning(t testing.TB, s stack, name string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.connect + "/connectors/" + name + "/status")
		if err == nil {
			var buf bytes.Buffer
			_, _ = io.Copy(&buf, resp.Body)
			_ = resp.Body.Close()
			if strings.Contains(buf.String(), `"tasks":[{"id":0,"state":"RUNNING"`) {
				return
			}
			if strings.Contains(buf.String(), `"state":"FAILED"`) {
				t.Fatalf("the connector failed: %s", buf.String())
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("connector %s never started running", name)
}

// realCDC is a real cdc service on the test's Kafka, shared group with its peers.
func (s stack) realCDC(t testing.TB, group, prefix string) cdc.ICDCService {
	t.Helper()
	svc := cdc.NewCDCService([]string{s.broker}, group, prefix)
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("cdc Run: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = svc.Stop(ctx)
	})
	return svc
}

func realService(t testing.TB, p *pair, c cdc.ICDCService) cqrs.ICQRSService[Account, AccountResponse, struct{}, int64] {
	t.Helper()
	return newService(t, p, nil, func(cfg *cqrsSvc) {
		cfg.CDCService = c
		cfg.Topic = "" // derived from the model: <prefix>.public.accounts
		cfg.BatchSize = 200
	})
}

// ---- tests -----------------------------------------------------------------

// The whole pipeline, with the data the connector's snapshot finds, then live
// inserts, updates and deletes.
func TestE2E_SnapshotThenLiveChanges(t *testing.T) {
	t.Parallel()
	s := skipUnlessDebezium(t)
	p := newPair(t, 8)
	p.exec(p.write, `INSERT INTO accounts (id, owner, balance, version) VALUES (1,'pre-1',10,1), (2,'pre-2',20,1)`)
	prefix := s.connector(t, p)

	svc := realService(t, p, s.realCDC(t, fmt.Sprintf("g-e2e-%d", runID), prefix))
	start(t, svc)

	eventually(t, "the snapshot rows", func() bool { return len(p.readAccounts()) == 2 })
	p.exec(p.write, `INSERT INTO accounts (id, owner, balance, version, note, active) VALUES (3, 'live', 30, 1, 'hello', true)`)
	p.exec(p.write, `UPDATE accounts SET balance = 99, version = 2 WHERE id = 1`)
	p.exec(p.write, `DELETE FROM accounts WHERE id = 2`)
	eventually(t, "live changes", func() bool {
		got := p.readAccounts()
		_, two := got[2]
		return len(got) == 2 && got[1].Balance == 99 && got[3].Owner == "live" && !two
	})
	got := p.readAccounts()
	if got[3].Note == nil || *got[3].Note != "hello" || !got[3].Active || got[3].Version != 1 {
		t.Fatalf("row 3 = %+v", got[3])
	}
	if got[1].UpdatedAt.IsZero() != false && got[1].UpdatedAt.Year() < 1970 {
		t.Fatalf("timestamptz did not survive: %v", got[1].UpdatedAt)
	}
	if svc.Stats().DeadLettered != 0 {
		t.Fatalf("nothing should be dead-lettered: %+v", svc.Stats())
	}
}

// Every column, in every state Postgres can hold it, survives the real trip
// through Debezium's JSON: nulls, zero values, extremes, unicode, and a row
// that only changed in a column the model does not map.
func TestE2E_ValuesSurviveTheRealWire(t *testing.T) {
	t.Parallel()
	s := skipUnlessDebezium(t)
	p := newPair(t, 8)
	prefix := s.connector(t, p)
	svc := realService(t, p, s.realCDC(t, fmt.Sprintf("g-e2e-vals-%d", runID), prefix))
	start(t, svc)

	p.exec(p.write, `INSERT INTO accounts (id, owner, balance, note, active, updated_at, version) VALUES
		(10, '', 0, NULL, false, '2026-03-04 05:06:07.891234+00', 0),
		(11, 'Zoë 日本語 🎉', 9223372036854775807, 'x', true, '2026-12-31 23:59:59.999999+08', 5),
		(12, 'neg', -9223372036854775808, '', false, '1970-01-01 00:00:00+00', 1)`)
	eventually(t, "3 rows", func() bool { return len(p.readAccounts()) == 3 })
	got := p.readAccounts()
	if got[10].Owner != "" || got[10].Balance != 0 || got[10].Note != nil {
		t.Fatalf("zero/null row = %+v", got[10])
	}
	if want := time.Date(2026, 3, 4, 5, 6, 7, 891234000, time.UTC); !got[10].UpdatedAt.Equal(want) {
		t.Fatalf("microsecond timestamp = %v", got[10].UpdatedAt)
	}
	if got[11].Owner != "Zoë 日本語 🎉" || got[11].Balance != 9223372036854775807 {
		t.Fatalf("extreme/unicode row = %+v", got[11])
	}
	if want := time.Date(2026, 12, 31, 15, 59, 59, 999999000, time.UTC); !got[11].UpdatedAt.Equal(want) {
		t.Fatalf("timestamptz with an offset = %v", got[11].UpdatedAt)
	}
	if got[12].Balance != -9223372036854775808 || got[12].Note == nil || *got[12].Note != "" {
		t.Fatalf("min int / empty-string row = %+v", got[12])
	}
}

// Schema evolution against the live pipeline: a column added to the write table
// is ignored by a service that does not know it yet, and replication carries on.
func TestE2E_SchemaEvolutionWhileRunning(t *testing.T) {
	t.Parallel()
	s := skipUnlessDebezium(t)
	p := newPair(t, 8)
	prefix := s.connector(t, p)
	svc := realService(t, p, s.realCDC(t, fmt.Sprintf("g-e2e-evo-%d", runID), prefix))
	start(t, svc)

	p.exec(p.write, `INSERT INTO accounts (id, owner, version) VALUES (1, 'before', 1)`)
	eventually(t, "the first row", func() bool { return len(p.readAccounts()) == 1 })
	p.exec(p.write, `ALTER TABLE accounts ADD COLUMN nickname text DEFAULT 'none'`)
	p.exec(p.write, `INSERT INTO accounts (id, owner, version, nickname) VALUES (2, 'after-add', 1, 'nick')`)
	p.exec(p.write, `UPDATE accounts SET owner = 'changed', version = 2 WHERE id = 1`)
	eventually(t, "changes after the ALTER", func() bool {
		got := p.readAccounts()
		return len(got) == 2 && got[1].Owner == "changed" && got[2].Owner == "after-add"
	})
	p.exec(p.write, `ALTER TABLE accounts DROP COLUMN nickname`)
	p.exec(p.write, `INSERT INTO accounts (id, owner, version) VALUES (3, 'after-drop', 1)`)
	eventually(t, "changes after the DROP", func() bool { return len(p.readAccounts()) == 3 })
	if svc.Stats().DeadLettered != 0 {
		t.Fatalf("schema changes must not dead-letter: %+v", svc.Stats())
	}
}

// Two replicas of the app share the stream through Kafka's consumer group: each
// gets partitions, together they apply every change exactly once, and when one
// stops mid-stream the other takes over with nothing lost.
func TestE2E_TwoReplicasShareTheStream(t *testing.T) {
	t.Parallel()
	s := skipUnlessDebezium(t)
	p := newPair(t, 16)
	prefix := s.connector(t, p)
	group := fmt.Sprintf("g-e2e-two-%d", runID)

	cdcA, cdcB := s.realCDC(t, group, prefix), s.realCDC(t, group, prefix)
	svcA, svcB := realService(t, p, cdcA), realService(t, p, cdcB)
	start(t, svcA)
	start(t, svcB)

	// Insert in waves until BOTH replicas have applied something: that is what
	// sharing the stream means. (A replica that joins the group late first sees a
	// rebalance, so the first wave may all go to one of them.)
	n := 0
	shared := false
	for wave := 0; wave < 30 && !shared; wave++ {
		var vals []string
		for range 50 {
			n++
			vals = append(vals, fmt.Sprintf("(%d, 'row-%d', %d, 1)", n, n, n))
		}
		p.exec(p.write, `INSERT INTO accounts (id, owner, balance, version) VALUES `+strings.Join(vals, ","))
		w := n
		eventually(t, "the wave on the read side", func() bool { return p.count(p.read, "accounts") == w })
		shared = svcA.Stats().Applied > 0 && svcB.Stats().Applied > 0
	}
	if !shared {
		t.Fatalf("the stream was never shared: A applied %d, B applied %d", svcA.Stats().Applied, svcB.Stats().Applied)
	}
	t.Logf("shared the stream: A applied %d, B applied %d (of %d)", svcA.Stats().Applied, svcB.Stats().Applied, n)

	// One replica goes away; changes made afterwards still arrive.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := svcA.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	p.exec(p.write, `UPDATE accounts SET balance = balance + 1000, version = 2`)
	eventually(t, "every row updated by the survivor", func() bool {
		var done int
		_ = p.read.Client().NewRaw(`SELECT count(*) FROM accounts WHERE version = 2`).Scan(context.Background(), &done)
		return done == n
	})
	got := p.readAccounts()
	for _, id := range []int64{1, int64(n / 2), int64(n)} {
		if got[id].Balance != id+1000 {
			t.Fatalf("row %d balance = %d, want %d", id, got[id].Balance, id+1000)
		}
	}
	a, b := svcA.Stats(), svcB.Stats()
	if a.DeadLettered+b.DeadLettered != 0 {
		t.Fatalf("nothing should be dead-lettered: %+v %+v", a, b)
	}
	t.Logf("final: A applied %d, B applied %d", a.Applied, b.Applied)
}

// A tombstone after a delete is skipped, not mistaken for a change, and the row
// is deleted exactly once even though two records (delete and tombstone) arrive.
func TestE2E_DeleteAndTombstone(t *testing.T) {
	t.Parallel()
	s := skipUnlessDebezium(t)
	p := newPair(t, 8)
	prefix := s.connector(t, p)
	svc := realService(t, p, s.realCDC(t, fmt.Sprintf("g-e2e-del-%d", runID), prefix))
	start(t, svc)
	p.exec(p.write, `INSERT INTO accounts (id, owner, version) VALUES (1, 'a', 1), (2, 'b', 1)`)
	eventually(t, "2 rows", func() bool { return len(p.readAccounts()) == 2 })
	p.exec(p.write, `DELETE FROM accounts WHERE id = 1`)
	eventually(t, "the tombstone to be seen", func() bool { return svc.Stats().Tombstones >= 1 })
	if got := p.readAccounts(); len(got) != 1 || got[2].Owner != "b" {
		t.Fatalf("read db = %+v", got)
	}
	if svc.Stats().DeadLettered != 0 {
		t.Fatalf("a tombstone is not poison: %+v", svc.Stats())
	}
}
