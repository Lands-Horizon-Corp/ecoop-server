//go:build integration

package cdc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/suitelock"
)

// Tests run against the compose stack, configured from .env or the
// environment (BROKER_HOST, BROKER_PORT, POSTGRES_*, CDC_*). The tests
// that feed hand-made events need only Kafka; the realtime ones also need
// Debezium and skip without it.
var (
	addr, skipReason, debeziumSkip string
	root                           string
	env                            = map[string]string{}
	seq                            atomic.Int64
	runID                          = time.Now().UnixNano()
)

var _ cdc.ICDCService = (*cdc.CDCService)(nil)

func TestMain(m *testing.M) {
	// The cdc suite writes to the shared Postgres and restarts Debezium through
	// Docker, which starves the other heavy suites: take turns with them.
	release, lockErr := suitelock.Acquire("cdc tests", 2*time.Hour)
	if lockErr != nil {
		fmt.Fprintln(os.Stderr, "cdc tests:", lockErr)
		os.Exit(1)
	}
	if err := start(); err != nil {
		skipReason = "no Kafka: " + err.Error()
		fmt.Fprintln(os.Stderr, "cdc tests:", skipReason)
	} else if err := startDebezium(); err != nil {
		debeziumSkip = "no Debezium: " + err.Error()
		fmt.Fprintln(os.Stderr, "cdc tests:", debeziumSkip)
	}
	code := m.Run()
	release()
	os.Exit(code)
}

func start() error {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		if parent := filepath.Dir(dir); parent != dir {
			dir = parent
		} else {
			return errors.New("go.mod not found")
		}
	}
	root = dir
	if data, err := os.ReadFile(filepath.Join(dir, ".env")); err == nil {
		for line := range strings.SplitSeq(string(data), "\n") {
			if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok && !strings.HasPrefix(k, "#") {
				env[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
			}
		}
	}
	for _, k := range []string{"BROKER_HOST", "BROKER_PORT"} {
		if v, ok := os.LookupEnv(k); ok {
			env[k] = v
		}
		if env[k] == "" {
			return fmt.Errorf("set %s in .env or the environment", k)
		}
	}
	addr = net.JoinHostPort(env["BROKER_HOST"], env["BROKER_PORT"])
	return compose("up", "-d", "--wait", "kafka")
}

func startDebezium() error {
	if err := compose("up", "-d", "--wait", "postgres", "debezium"); err != nil {
		return err
	}
	// The realtime tests write to public.users; the stack does not ship that table.
	if err := compose("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1",
		"-U", cfg("POSTGRES_USER", "ecoop"), "-d", cfg("POSTGRES_DB", "ecoop"), "-c",
		`CREATE TABLE IF NOT EXISTS users (id bigserial PRIMARY KEY, name text NOT NULL)`); err != nil {
		return err
	}
	if err := compose("up", "debezium-setup"); err != nil { // idempotent: PUTs the connector config
		return err
	}
	// A task that started before the table existed stays FAILED; restart it.
	url := fmt.Sprintf("http://localhost:%s/connectors/%s/restart?includeTasks=true&onlyFailed=true",
		cfg("CDC_PORT", "8083"), cfg("CDC_CONNECTOR_NAME", "ecoop-postgres"))
	if resp, err := http.Post(url, "application/json", nil); err == nil {
		resp.Body.Close()
	}
	return nil
}

func compose(args ...string) error {
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return nil
}

// cfg returns env[k] (the process environment wins), or def.
func cfg(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	if v := env[k]; v != "" {
		return v
	}
	return def
}

// psql runs sql in the compose Postgres, as the app's database user.
func psql(t testing.TB, sql string) {
	t.Helper()
	out, err := exec.Command("docker", "compose", "-f", filepath.Join(root, "docker-compose.yml"),
		"exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1",
		"-U", cfg("POSTGRES_USER", "ecoop"), "-d", cfg("POSTGRES_DB", "ecoop"), "-c", sql).CombinedOutput()
	if err != nil {
		t.Fatalf("psql: %v\n%s", err, out)
	}
}

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// topic is unique to the test; the service gives each topic its own group.
func names() string {
	return fmt.Sprintf("cdc-test-%d-%d", runID, seq.Add(1))
}

func skipUnlessKafka(t testing.TB) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

func skipUnlessDebezium(t testing.TB) {
	t.Helper()
	skipUnlessKafka(t)
	if debeziumSkip != "" {
		t.Skip(debeziumSkip)
	}
}

// newService is a CDC service for the test Kafka, not yet running.
func newService() cdc.ICDCService {
	return cdc.NewCDCService([]string{addr}, fmt.Sprintf("cdc-test-%d", runID), cfg("CDC_TOPIC_PREFIX", "ecoop"))
}

// service is a CDC service, run until t ends.
func service(t testing.TB) cdc.ICDCService {
	t.Helper()
	skipUnlessKafka(t)
	s := newService()
	if err := s.Run(ctxT(t)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s
}

// pub publishes hand-made Debezium messages straight to Kafka.
type pub struct{ c *kgo.Client }

func (p pub) Publish(ctx context.Context, topic string, key, value []byte) error {
	return p.c.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).FirstErr()
}

func producer(t testing.TB) pub {
	t.Helper()
	skipUnlessKafka(t)
	c, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return pub{c}
}

// readAll collects n records from topic, read by a throwaway client.
func readAll(t testing.TB, topic string, n int) []*kgo.Record {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var got []*kgo.Record
	for len(got) < n {
		c.PollFetches(ctx).EachRecord(func(r *kgo.Record) { got = append(got, r) })
		if ctx.Err() != nil {
			t.Fatalf("%s holds %d of %d records", topic, len(got), n)
		}
	}
	return got
}

// countFor returns how many records topic holds after waiting d for more.
func countFor(t testing.TB, topic string, d time.Duration) int {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	n := 0
	for ctx.Err() == nil {
		c.PollFetches(ctx).EachRecord(func(*kgo.Record) { n++ })
	}
	return n
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// event is a Debezium value (no schema wrapper) for row id.
func event(op cdc.ChangeType, id int, name string) []byte {
	row := fmt.Sprintf(`{"id":%d,"name":%q}`, id, name)
	before, after := "null", row
	switch op {
	case cdc.OpUpdate:
		before = fmt.Sprintf(`{"id":%d}`, id)
	case cdc.OpDelete:
		before, after = fmt.Sprintf(`{"id":%d}`, id), "null"
	}
	return fmt.Appendf(nil, `{"op":%q,"before":%s,"after":%s,"ts_ms":1}`, op, before, after)
}

func key(id int) []byte { return fmt.Appendf(nil, `{"id":%d}`, id) }

type got struct {
	Key   string
	Op    cdc.ChangeType
	ID    int64
	Name  string
	After json.RawMessage
}

// inbox collects what a subscription receives.
type inbox struct {
	mu   sync.Mutex
	msgs []got
}

func (in *inbox) handle(_ context.Context, k json.RawMessage, ev cdc.ChangeEvent) error {
	raw := ev.After
	if ev.Op == cdc.OpDelete {
		raw = ev.Before
	}
	var row struct {
		ID   int64
		Name string
	}
	_ = json.Unmarshal(raw, &row)
	in.mu.Lock()
	defer in.mu.Unlock()
	in.msgs = append(in.msgs, got{Key: string(k), Op: ev.Op, ID: row.ID, Name: row.Name, After: ev.After})
	return nil
}

func (in *inbox) all() []got {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]got(nil), in.msgs...)
}

// wait polls until in holds n messages.
func (in *inbox) wait(t testing.TB, n int) []got {
	t.Helper()
	return in.waitUntil(t, fmt.Sprintf("%d messages", n), func(g []got) bool { return len(g) >= n })
}

func (in *inbox) waitUntil(t testing.TB, what string, done func([]got) bool) []got {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !done(in.all()) {
		if time.Now().After(deadline) {
			t.Fatalf("never saw %s; have %+v", what, in.all())
		}
		time.Sleep(50 * time.Millisecond)
	}
	return in.all()
}

func mustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s: expected panic", name)
		}
	}()
	fn()
}
