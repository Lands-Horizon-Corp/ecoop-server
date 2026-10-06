package regressions

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
)

// 14 Failover: after a primary is demoted it stays reachable but read-only, so writes must fail fast
// and cleanly while reads keep working, and everything must recover once a writable primary is back.
// The full container restart is opt-in (DOCKER_CHAOS=1, `make test-chaos`) because it interrupts
// the shared Postgres.

func TestBankDBFailover_DemotedPrimaryRejectsWritesAndRecovers(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true, app: "demoted"})
	b.open("a", 1000)
	b.open("b", 0)
	ctx := withDeadline(t, 20*time.Second)
	writerDB := strings.TrimPrefix(mustURLPath(t, b.h.writerDSN), "/")
	admin := openInspect(t, envOr("SQL_TEST_DSN", defaultPostgresDSN))
	demote := func(readOnly bool) {
		_, err := admin.Exec(fmt.Sprintf(`ALTER DATABASE %s SET default_transaction_read_only = %t`, writerDB, readOnly))
		must(t, err)
		// Sessions only pick the setting up when they start, as after a real failover.
		_, err = admin.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'demoted-w'`)
		must(t, err)
	}

	demote(true)
	_, _, err := b.Transfer(ctx, transferReq{Key: "during", From: "a", To: "b", Amount: 1})
	requireKind(t, err, database.ErrUnavailable) // 25006, not an internal error
	if _, err := b.accounts.Find(ctx, eqFilter("id", "a")); err != nil {
		t.Fatalf("reads stopped working while the writer was read-only: %v", err)
	}
	if got := b.writerBalance("a"); got != 1000 {
		t.Fatalf("balance a = %d; a write slipped through", got)
	}

	demote(false)
	if _, _, err := b.Transfer(ctx, transferReq{Key: "after", From: "a", To: "b", Amount: 1}); err != nil {
		t.Fatalf("writes did not recover after the primary came back: %v", err)
	}
}

func TestBankDBFailover_PostgresRestartMidWorkload(t *testing.T) {
	if os.Getenv("DOCKER_CHAOS") == "" {
		t.Skip("restarts the shared Postgres container; run with DOCKER_CHAOS=1 (make test-chaos)")
	}
	b := newBDBank(t, bdOpts{noRun: true, app: "restart"})
	ids := []string{"a", "b", "c", "d"}
	for _, id := range ids {
		b.open(id, 10_000)
	}
	var restarted atomic.Bool
	go func() {
		time.Sleep(200 * time.Millisecond)
		out, err := composeCmd("restart", "postgres").CombinedOutput()
		if err != nil {
			t.Errorf("docker compose restart postgres: %v\n%s", err, out)
		}
		restarted.Store(true)
	}()

	var committed atomic.Int64
	_ = runParallel(t, 300, func(ctx context.Context, i int) error {
		req := transferReq{Key: fmt.Sprintf("k%03d", i), From: ids[i%4], To: ids[(i+1)%4], Amount: 1}
		for attempt := 0; ; attempt++ { // a client retrying with its idempotency key
			_, replayed, err := b.Transfer(ctx, req)
			if err == nil {
				if !replayed {
					committed.Add(1)
				}
				return nil
			}
			if !errors.Is(err, database.ErrUnavailable) && !errors.Is(err, database.ErrTimeout) {
				t.Errorf("transfer %d: %v", i, err)
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
	if !restarted.Load() {
		t.Fatal("the restart never happened")
	}
	if n := count(t, openInspect(t, b.h.writerDSN), `SELECT count(*) FROM bank_transfers`); n != 300 {
		t.Fatalf("%d transfers recorded; want 300 (each exactly once)", n)
	}
	if total := count(t, openInspect(t, b.h.writerDSN), `SELECT SUM(balance) FROM bank_accounts`); total != 40_000 {
		t.Fatalf("total = %d; want 40000", total)
	}
}

// composeCmd runs docker compose on this repository's compose file, whatever the working directory
// (the harness moves each test into a temp dir).
func composeCmd(args ...string) *exec.Cmd {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	return exec.Command("docker", append([]string{"compose", "-f", filepath.Join(root, "docker-compose.yml")}, args...)...)
}
