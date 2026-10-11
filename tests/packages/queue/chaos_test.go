package queue_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

// Chaos: random handler outcomes, dropped connections and lifecycle churn.

func seed(t *testing.T) uint64 {
	s := uint64(time.Now().UnixNano())
	t.Logf("chaos seed: %d", s)
	return s
}

func TestChaos_RandomOutcomesAllSettle(t *testing.T) {
	// Every job either succeeds (eventually) or is archived; none is lost
	// or stuck.
	q := newQueue(t)
	s := seed(t)
	typ := taskType(t, "random")
	var mu sync.Mutex
	succeeded := map[int]bool{}
	attempts := map[int]int{}
	q.Handle(typ, queue.JSON(func(_ context.Context, id int) error {
		mu.Lock()
		attempts[id]++
		n := attempts[id]
		mu.Unlock()
		r := rand.New(rand.NewPCG(s, uint64(id*31+n)))
		switch r.IntN(4) {
		case 0:
			return errors.New("transient")
		case 1:
			return queue.RetryAfter(time.Duration(r.IntN(300))*time.Millisecond, errors.New("later"))
		case 2:
			return errors.Join(errors.New("give up"), queue.ErrSkipRetry)
		}
		mu.Lock()
		succeeded[id] = true
		mu.Unlock()
		return nil
	}))
	const n = 30
	for i := range n {
		if _, err := q.Enqueue(ctxT(t), typ, i, queue.MaxRetry(2)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 45*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(succeeded)+archived(t, queue.Default) >= n
	}, "jobs did not all settle (succeeded or archived)")
}

func TestChaos_ConnectionDropsLoseNoQueuedJob(t *testing.T) {
	// A connection cut mid-job leaves it "active" until asynq's recoverer
	// reclaims it (lease expiry, up to ~90s). So every acknowledged job must
	// have run or still be in the queue to be retried, never vanish.
	p := newProxy(t)
	q := newQueue(t, p.Addr())
	typ := taskType(t, "drops")
	var mu sync.Mutex
	ran := map[string]bool{}
	q.Handle(typ, queue.JSON(func(_ context.Context, id string) error {
		mu.Lock()
		ran[id] = true
		mu.Unlock()
		return nil
	}))
	queued := map[string]string{} // payload -> job ID
	for i := range 40 {
		key := fmt.Sprint(i)
		if id, err := q.Enqueue(ctxT(t), typ, key); err == nil {
			queued[key] = id
		}
		if i%10 == 5 {
			p.DropAll()
		}
	}
	t.Logf("%d of 40 enqueues acknowledged across drops", len(queued))
	time.Sleep(3 * time.Second)
	in := inspector()
	defer in.Close()
	mu.Lock()
	defer mu.Unlock()
	pending := 0
	for key, id := range queued {
		if ran[key] {
			continue
		}
		if _, err := in.GetTaskInfo(queue.Default, id); err != nil {
			t.Errorf("job %s (%s) neither ran nor is in the queue: %v", key, id, err)
		}
		pending++
	}
	t.Logf("%d ran, %d still queued for recovery", len(queued)-pending, pending)
}

func TestChaos_LifecycleChurnUnderLoad(t *testing.T) {
	q := newQueue(t)
	typ := taskType(t, "churn")
	var mu sync.Mutex
	ran := map[string]bool{}
	q.Handle(typ, queue.JSON(func(_ context.Context, id string) error {
		mu.Lock()
		ran[id] = true
		mu.Unlock()
		return nil
	}))
	var qmu sync.Mutex
	var queued []string
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				case <-time.After(5 * time.Millisecond):
				}
				id := fmt.Sprintf("%d-%d", w, i)
				if _, err := q.Enqueue(context.Background(), typ, id); err == nil {
					qmu.Lock()
					queued = append(queued, id)
					qmu.Unlock()
				} else if !errors.Is(err, queue.ErrNotStarted) {
					t.Errorf("Enqueue: %v", err)
				}
			}
		})
	}
	for range 6 {
		time.Sleep(40 * time.Millisecond)
		if err := q.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := q.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	t.Logf("%d jobs queued across 6 restarts", len(queued))
	eventually(t, 30*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, id := range queued {
			if !ran[id] {
				return false
			}
		}
		return true
	}, "a job queued during churn never ran")
}
