package inflight_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Integration chaos: Inflight used the way services use it: a running flag
// guarded by a mutex, operations tracked while they run, Stop that refuses
// new work then drains, under random load and start/stop churn.

var errNotRunning = errors.New("not running")

type fakeService struct {
	mu       sync.RWMutex
	running  bool
	inflight *utils.Inflight
	active   atomic.Int64 // operations executing right now
	closed   atomic.Bool  // resources released
}

func newFakeService(f *utils.Inflight) *fakeService {
	return &fakeService{inflight: f, running: true}
}

func (s *fakeService) Do(work time.Duration) error {
	s.mu.RLock()
	if !s.running {
		s.mu.RUnlock()
		return errNotRunning
	}
	done := s.inflight.Track()
	s.mu.RUnlock()
	defer done()

	s.active.Add(1)
	defer s.active.Add(-1)
	if s.closed.Load() {
		return errors.New("used after resources were released")
	}
	time.Sleep(work)
	if s.closed.Load() {
		return errors.New("resources released mid-operation")
	}
	return nil
}

func (s *fakeService) Stop(ctx context.Context) error {
	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
	if err := s.inflight.Wait(ctx); err != nil {
		return err
	}
	s.closed.Store(true)
	return nil
}

func (s *fakeService) Start() {
	s.mu.Lock()
	s.running = true
	s.closed.Store(false)
	s.mu.Unlock()
}

func TestIntegrationChaos_StopNeverReleasesUnderAnOperation(t *testing.T) {
	t.Parallel()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	for round := range 20 {
		svc := newFakeService(utils.NewInflight())
		var wg sync.WaitGroup
		for i := range 64 {
			wg.Go(func() {
				r := rand.New(rand.NewPCG(seed, uint64(round*1000+i)))
				for range 10 {
					if err := svc.Do(time.Duration(r.IntN(2000)) * time.Microsecond); err != nil && !errors.Is(err, errNotRunning) {
						t.Errorf("round %d: %v", round, err)
					}
				}
			})
		}
		time.Sleep(time.Duration(rand.IntN(3)) * time.Millisecond)
		if err := svc.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if a := svc.active.Load(); a != 0 {
			t.Fatalf("round %d: Stop returned with %d operations still running", round, a)
		}
		wg.Wait()
	}
}

func TestIntegrationChaos_SharedTrackerDrainsAllServices(t *testing.T) {
	t.Parallel()
	shared := utils.NewInflight()
	a, b := newFakeService(shared), newFakeService(shared)

	slowB := make(chan error, 1)
	go func() { slowB <- b.Do(150 * time.Millisecond) }()
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	if err := a.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("a.Stop took %v; with a shared tracker it should wait for b's operation", d)
	}
	if err := <-slowB; err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationChaos_SeparateTrackersDrainIndependently(t *testing.T) {
	t.Parallel()
	a, b := newFakeService(utils.NewInflight()), newFakeService(utils.NewInflight())
	go func() { _ = b.Do(300 * time.Millisecond) }()
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	if err := a.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("a.Stop waited %v on b's operation despite separate trackers", d)
	}
	_ = b.Stop(context.Background())
}

func TestIntegrationChaos_StartStopChurn(t *testing.T) {
	t.Parallel()
	svc := newFakeService(utils.NewInflight())
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := svc.Do(200 * time.Microsecond)
				if errors.Is(err, errNotRunning) {
					// Back off like a real caller; a tight retry loop starves
					// the operations Stop is waiting for under -race and load.
					time.Sleep(100 * time.Microsecond)
				} else if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	for range 30 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := svc.Stop(ctx); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		cancel()
		if a := svc.active.Load(); a != 0 {
			t.Fatalf("Stop returned with %d active", a)
		}
		svc.Start()
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
}
