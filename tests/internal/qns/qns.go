// Package qns gives each test its own queue namespace, so tests that run
// queue workers on the shared dev Valkey can all run in parallel without
// picking up each other's jobs, cron leadership or rate limits.
package qns

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/valkey-io/valkey-go"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

var (
	runID = time.Now().UnixNano()
	seq   atomic.Int64
	byT   sync.Map // testing.TB -> namespace
)

// For returns t's namespace: the same for every call within t, so a test's
// queue instances share work like replicas of one app, and unique across
// tests and runs. Its queues are deleted when t ends.
func For(t testing.TB, addr, password string) string {
	t.Helper()
	if ns, ok := byT.Load(t); ok {
		return ns.(string)
	}
	ns := fmt.Sprintf("test-%d-%d", runID, seq.Add(1))
	if actual, loaded := byT.LoadOrStore(t, ns); loaded {
		return actual.(string)
	}
	t.Cleanup(func() {
		byT.Delete(t)
		Purge(addr, password, ns)
	})
	return ns
}

// Inspector opens the queue database of the Valkey at addr.
func Inspector(addr, password string) *asynq.Inspector {
	return asynq.NewInspector(asynq.RedisClientOpt{Addr: addr, Password: password, DB: queue.DB})
}

// Purge deletes namespace ns's queues, retrying while a worker that is still
// shutting down holds an active job. A job orphaned "active" by a dropped
// connection is never released once the namespace's workers are gone, so
// after the deadline the queue's keys are removed directly; the namespace
// belongs to one test, so nothing else can be using them.
func Purge(addr, password, ns string) {
	in := Inspector(addr, password)
	defer in.Close()
	deadline := time.Now().Add(queue.ShutdownTimeout + 5*time.Second)
	for _, base := range []string{queue.Critical, queue.Default, queue.Low} {
		name := queue.QueueName(ns, base)
		for {
			err := in.DeleteQueue(name, true)
			if err == nil || errors.Is(err, asynq.ErrQueueNotFound) {
				break
			}
			if time.Now().After(deadline) {
				forceDelete(addr, password, name)
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func forceDelete(addr, password, name string) {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, Password: password, SelectDB: queue.DB})
	if err != nil {
		return
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cursor uint64
	for {
		entry, err := client.Do(ctx, client.B().Scan().Cursor(cursor).Match("asynq:{"+name+"}:*").Count(500).Build()).AsScanEntry()
		if err != nil {
			return
		}
		if len(entry.Elements) > 0 {
			_ = client.Do(ctx, client.B().Del().Key(entry.Elements...).Build()).Error()
		}
		if cursor = entry.Cursor; cursor == 0 {
			break
		}
	}
	_ = client.Do(ctx, client.B().Srem().Key("asynq:queues").Member(name).Build()).Error()
}
