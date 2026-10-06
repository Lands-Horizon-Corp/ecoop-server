package regressions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broker"
)

// Real change-data-capture chain for the CDC suite (`make test-up-cdc`):
//
//	Postgres 16 writer (logical replication) -> Debezium Connect -> Kafka -> runner -> Postgres 16 reader
//
// Each test gets its own writer and reader databases, its own Debezium connector (unique slot,
// publication and topic prefix) and its own consumer group, so tests never see each other's changes.

type cdcStream struct {
	prefix  string // Debezium topic prefix; topics are <prefix>.public.<table>
	brokers []string
	kafka   broker.BatchBrokerServices
}

// newCDCBank is newBDBank on the Postgres 16 pair with real Kafka, plus a Debezium connector
// streaming the writer's bank tables.
func newCDCBank(t *testing.T, o bdOpts) (*bdLedger, *cdcStream) {
	t.Helper()
	for _, addr := range []string{hostOf(envOr("CDC_WRITE_DSN", defaultCDCWriteDSN)), hostOf(envOr("CDC_READ_DSN", defaultCDCReadDSN)),
		envOr("CDC_KAFKA_BROKERS", "localhost:9094"), hostOf(envOr("CDC_CONNECT_URL", "http://localhost:8083"))} {
		requireReachableHint(t, addr, "make test-up-cdc")
	}
	s := &cdcStream{
		prefix:  fmt.Sprintf("t%d", time.Now().UnixNano()),
		brokers: strings.Split(envOr("CDC_KAFKA_BROKERS", "localhost:9094"), ","),
	}
	s.kafka = s.newBroker(t, s.prefix+"-runner")
	o.target, o.broker, o.channelPrefix = "pg16", s.kafka, s.prefix+".public."
	b := newBDBank(t, o)
	s.registerConnector(t, b)
	return b, s
}

func (s *cdcStream) newBroker(t *testing.T, group string) broker.BatchBrokerServices {
	t.Helper()
	k := broker.NewBrokerService(s.brokers, group, broker.Options{
		Consumer: broker.ConsumerOptions{BatchSize: 50, BatchWait: 50 * time.Millisecond},
		Producer: broker.ProducerOptions{BatchSize: 10, BatchWait: 10 * time.Millisecond},
	}, nil)
	must(t, k.Run(bg))
	t.Cleanup(func() { _ = k.Stop(bg) })
	return k
}

func (s *cdcStream) topic(table string) string { return s.prefix + ".public." + table }

func (s *cdcStream) registerConnector(t *testing.T, b *bdLedger) {
	t.Helper()
	u, err := url.Parse(b.h.writerDSN)
	must(t, err)
	dbName := strings.TrimPrefix(u.Path, "/")
	slot := s.prefix + "_slot"
	cfg := map[string]any{
		"name": s.prefix,
		"config": map[string]string{
			"connector.class":                "io.debezium.connector.postgresql.PostgresConnector",
			"database.hostname":              "pg16-write",
			"database.port":                  "5432",
			"database.user":                  "ecoop",
			"database.password":              "ecoop-test-pass",
			"database.dbname":                dbName,
			"topic.prefix":                   s.prefix,
			"plugin.name":                    "pgoutput",
			"slot.name":                      slot,
			"publication.name":               s.prefix + "_pub",
			"publication.autocreate.mode":    "filtered",
			"table.include.list":             "public.bank_accounts,public.bank_transfers,public.bank_audit",
			"tombstones.on.delete":           "false",
			"key.converter":                  "org.apache.kafka.connect.json.JsonConverter",
			"key.converter.schemas.enable":   "false",
			"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
			"value.converter.schemas.enable": "false",
			"poll.interval.ms":               "50",
		},
	}
	connect := envOr("CDC_CONNECT_URL", "http://localhost:8083")
	body, _ := json.Marshal(cfg)
	resp, err := http.Post(connect+"/connectors", "application/json", bytes.NewReader(body))
	must(t, err)
	msg, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("registering the Debezium connector: %s %s", resp.Status, msg)
	}
	t.Cleanup(func() {
		req, _ := http.NewRequest(http.MethodDelete, connect+"/connectors/"+s.prefix, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
		// The slot pins WAL on the shared server; it can only be dropped once Debezium lets go of it.
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := b.h.writer.Exec(`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, slot); err == nil {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Errorf("replication slot %s was not released", slot)
	})

	deadline := time.Now().Add(60 * time.Second)
	for {
		state := s.connectorState(connect)
		if state == "RUNNING" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Debezium connector %s did not start: %s", s.prefix, state)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (s *cdcStream) connectorState(connect string) string {
	resp, err := http.Get(connect + "/connectors/" + s.prefix + "/status")
	if err != nil {
		return err.Error()
	}
	defer resp.Body.Close()
	var st struct {
		Connector struct{ State string } `json:"connector"`
		Tasks     []struct {
			State string `json:"state"`
			Trace string `json:"trace"`
		} `json:"tasks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return err.Error()
	}
	if len(st.Tasks) == 0 {
		return "no tasks (" + st.Connector.State + ")"
	}
	if st.Tasks[0].State == "FAILED" {
		return "FAILED: " + st.Tasks[0].Trace
	}
	return st.Tasks[0].State
}

// collect subscribes to topic on its own consumer group and returns everything received so far.
func (s *cdcStream) collect(t *testing.T, topic string) func() []broker.Message {
	t.Helper()
	k := s.newBroker(t, fmt.Sprintf("%s-collect-%d", s.prefix, time.Now().UnixNano()))
	var (
		mu  sync.Mutex
		got []broker.Message
	)
	ctx, cancel := context.WithCancel(bg)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = k.SubscribeBatch(ctx, topic, func(batch []broker.Message) error {
			mu.Lock()
			got = append(got, batch...)
			mu.Unlock()
			return nil
		})
	}()
	t.Cleanup(func() { cancel(); <-done })
	return func() []broker.Message {
		mu.Lock()
		defer mu.Unlock()
		return append([]broker.Message(nil), got...)
	}
}

// awaitMirror waits until every bank table on the reader matches the writer row for row.
func awaitMirror(t *testing.T, b *bdLedger) {
	t.Helper()
	queries := map[string]string{
		"bank_accounts":  `SELECT id || '|' || owner || '|' || currency || '|' || balance || '|' || version || '|' || COALESCE(profile::text, '-') || '|' || COALESCE(encode(signature, 'hex'), '-') || '|' || COALESCE(closed_at::text, '-') || '|' || updated_at FROM bank_accounts ORDER BY id`,
		"bank_transfers": `SELECT id || '|' || idempotency_key || '|' || from_account || '|' || to_account || '|' || amount || '|' || kind || '|' || updated_at FROM bank_transfers ORDER BY id`,
		"bank_audit":     `SELECT id || '|' || transfer_id || '|' || delta || '|' || balance_after FROM bank_audit ORDER BY id`,
	}
	deadline := time.Now().Add(60 * time.Second)
	for table, q := range queries {
		for {
			w, r := queryLines(t, b.h.writer, q), queryLines(t, b.h.reader, q)
			if w == r {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("reader %s never matched the writer:\nwriter: %s\nreader: %s", table, w, r)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.Host
}

// requireReachableHint is requireReachable with the make target that starts the service.
func requireReachableHint(t testing.TB, addr, target string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("%s is not reachable (%v); run: %s", addr, err, target)
	}
	_ = conn.Close()
}
