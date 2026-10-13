//go:build integration

package broker_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
)

// Tests run against the kafka service in docker-compose.yml, configured from
// BROKER_HOST and BROKER_PORT in the environment or the repo's .env. Each
// test uses its own topic and consumer group.
var (
	addr, skipReason string
	seq              atomic.Int64
	runID            = time.Now().UnixNano()
)

var _ broker.IBrokerService = (*broker.BrokerService)(nil)

func TestMain(m *testing.M) {
	if err := startKafka(); err != nil {
		skipReason = "no Kafka: " + err.Error()
		fmt.Fprintln(os.Stderr, "broker tests:", skipReason)
	}
	os.Exit(m.Run())
}

func startKafka() error {
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
	env := map[string]string{}
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
	up := exec.Command("docker", "compose", "up", "-d", "--wait", "kafka")
	up.Dir = dir
	if out, err := up.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up kafka: %w\n%s", err, out)
	}
	return nil
}

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// names are a topic and a consumer group unique to the test.
func names() (topic, group string) {
	n := seq.Add(1)
	return fmt.Sprintf("test-%d-%d", runID, n), fmt.Sprintf("group-%d-%d", runID, n)
}

// service is a broker in group, run until t ends.
func service(t testing.TB, group string) broker.IBrokerService {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	b := broker.NewBrokerService([]string{addr}, group, nil)
	if err := b.Run(ctxT(t)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	return b
}

// inbox collects what a subscription receives.
type inbox struct {
	mu   sync.Mutex
	msgs []string // "key=value"
}

func (in *inbox) handle(_ context.Context, key, value []byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.msgs = append(in.msgs, string(key)+"="+string(value))
	return nil
}

func (in *inbox) all() []string {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]string(nil), in.msgs...)
}

// wait polls until in holds n messages.
func (in *inbox) wait(t testing.TB, n int) []string {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for len(in.all()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("received %d of %d messages", len(in.all()), n)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return in.all()
}

func publish(t testing.TB, b broker.IBrokerService, topic string, n int, key string) {
	t.Helper()
	for i := range n {
		if err := b.Publish(ctxT(t), topic, []byte(key), []byte(fmt.Sprint(i))); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
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
