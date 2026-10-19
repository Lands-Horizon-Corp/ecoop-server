package lifecycle_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// journal records the order things start and stop in.
type journal struct {
	mu  sync.Mutex
	log []string
}

func (j *journal) add(s string) {
	j.mu.Lock()
	j.log = append(j.log, s)
	j.mu.Unlock()
}

func (j *journal) step(s string) func(context.Context) error {
	return func(context.Context) error { j.add(s); return nil }
}

type low struct{}
type high struct{}

// A service built from another starts after it and stops before it: the order
// every service in this repository depends on (the queue is up before the mailer).
func TestADependentStartsAfterAndStopsBeforeItsDependency(t *testing.T) {
	t.Parallel()
	var j journal
	app := fxtest.New(t, fx.NopLogger,
		fx.Provide(
			func(lc fx.Lifecycle) *low {
				utils.OnLifecycle(lc, j.step("start low"), j.step("stop low"))
				return &low{}
			},
			func(lc fx.Lifecycle, _ *low) *high {
				utils.OnLifecycle(lc, j.step("start high"), j.step("stop high"))
				return &high{}
			},
		),
		fx.Invoke(func(*high) {}),
	)
	app.RequireStart()
	app.RequireStop()
	want := []string{"start low", "start high", "stop high", "stop low"}
	if !slices.Equal(j.log, want) {
		t.Errorf("order = %v, want %v", j.log, want)
	}
}

func TestNilHooksAreAllowed(t *testing.T) {
	t.Parallel()
	app := fxtest.New(t, fx.NopLogger,
		fx.Provide(func(lc fx.Lifecycle) *low { utils.OnLifecycle(lc, nil, nil); return &low{} }),
		fx.Invoke(func(*low) {}),
	)
	app.RequireStart()
	app.RequireStop()
}

func TestSequenceRunsInOrderAndStopsAtTheFirstError(t *testing.T) {
	t.Parallel()
	var j journal
	boom := errors.New("boom")
	err := utils.Sequence(j.step("a"), nil, func(context.Context) error { j.add("b"); return boom }, j.step("c"))(context.Background())
	if !errors.Is(err, boom) {
		t.Errorf("error = %v", err)
	}
	if !slices.Equal(j.log, []string{"a", "b"}) {
		t.Errorf("steps run = %v, want a then b, and not c after the failure", j.log)
	}
	if err := utils.Sequence()(context.Background()); err != nil {
		t.Errorf("an empty sequence = %v", err)
	}
}

// A start that fails stops the application, and what had already started is
// stopped again.
func TestAFailedStartStopsWhatAlreadyStarted(t *testing.T) {
	t.Parallel()
	var j journal
	app := fx.New(fx.NopLogger,
		fx.Provide(
			func(lc fx.Lifecycle) *low {
				utils.OnLifecycle(lc, j.step("start low"), j.step("stop low"))
				return &low{}
			},
			func(lc fx.Lifecycle, _ *low) *high {
				utils.OnLifecycle(lc, func(context.Context) error { return errors.New("cannot connect") }, j.step("stop high"))
				return &high{}
			},
		),
		fx.Invoke(func(*high) {}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Start(ctx); err == nil {
		t.Fatal("a failed start did not fail the application")
	}
	if !slices.Contains(j.log, "stop low") {
		t.Errorf("the service that had started was not stopped: %v", j.log)
	}
	if slices.Contains(j.log, "stop high") {
		t.Errorf("a service that never started was stopped: %v", j.log)
	}
}
