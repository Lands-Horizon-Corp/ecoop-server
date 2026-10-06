package regressions

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
)

// 18 Change hooks (Dispatch / Broadcast after a change is applied) run on a bounded worker pool:
// a burst cannot spawn a goroutine per change, every hook still runs, and Stop drains them.

func TestBankDBHooks_BurstRunsOnABoundedPoolAndStopDrainsIt(t *testing.T) {
	h := newDBHarness(t)
	const workers = 4
	var running, peak, dispatched atomic.Int64
	svc := h.newService(h.writerDSN, h.readerDSN)
	must(t, database.Register(svc, database.Registration[dbMember, dbMemberResource, dbMemberRequest, string]{
		Channel:     "members",
		HookWorkers: workers,
		ToResource:  func(m *dbMember) *dbMemberResource { return &dbMemberResource{ID: m.ID} },
		Created:     func(*dbMember) broadcast.Events { return broadcast.Events{"member.created"} },
		Dispatch: func(broadcast.Channel, broadcast.Events, *dbMemberResource) error {
			n := running.Add(1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			time.Sleep(200 * time.Microsecond)
			running.Add(-1)
			dispatched.Add(1)
			return nil
		},
	}))
	must(t, svc.Start(bg))
	t.Cleanup(func() { _ = svc.Stop(bg) })
	members, err := database.Get[dbMember, dbMemberResource, dbMemberRequest, string](svc)
	must(t, err)

	before := runtime.NumGoroutine()
	var maxGoroutines atomic.Int64
	const events = 10_000
	errs := runParallel(t, 50, func(_ context.Context, i int) error {
		for j := range events / 50 {
			members.OnCreated(bg, &dbMember{ID: "m", Name: "n", Balance: int64(i*1000 + j)})
			if g := int64(runtime.NumGoroutine()); g > maxGoroutines.Load() {
				maxGoroutines.Store(g)
			}
		}
		return nil
	})
	if len(errs) > 0 {
		t.Fatal(errs[0])
	}
	must(t, svc.Stop(bg)) // drains whatever is still queued

	if got := dispatched.Load(); got != events {
		t.Fatalf("%d of %d hooks ran before Stop returned", got, events)
	}
	if p := peak.Load(); p > workers {
		t.Fatalf("%d hooks ran at once; the pool allows %d", p, workers)
	}
	// 50 producers + 4 workers + test machinery; one goroutine per event would be ~10,000.
	if g := maxGoroutines.Load(); g > int64(before)+50+workers+20 {
		t.Fatalf("goroutines peaked at %d (from %d): hooks are not bounded", g, before)
	}

	noPanic(t, "hook after Stop", func() { members.OnCreated(bg, &dbMember{ID: "late"}) })
	time.Sleep(20 * time.Millisecond)
	if got := dispatched.Load(); got != events {
		t.Fatalf("a hook ran after Stop (%d)", got)
	}
}
