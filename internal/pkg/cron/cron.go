package cron

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	robfig "github.com/robfig/cron/v3"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// CronService schedules jobs on the queue service, so the schedule survives
// a load balancer: every instance knows every job, the queue's leader
// enqueues each tick once, and any instance's worker runs it.
type CronService struct {
	queue queue.IQueueService
	gate  *utils.Gate
	obs   observability.IObserver

	mu   sync.Mutex // guards jobs and loc
	jobs map[string]*job
	loc  *time.Location
}

type job struct {
	CronJob
	schedule string // the queue's schedule id
}

var parser = robfig.NewParser(robfig.Minute | robfig.Hour | robfig.Dom | robfig.Month | robfig.Dow | robfig.Descriptor)

// NewCronService creates the service. inflight may be shared with other
// services; nil gives the service its own tracker.
func NewCronService(queue queue.IQueueService, inflight *utils.Inflight, opts ...Option) ICronServices {
	c := &CronService{
		queue: queue,
		gate:  utils.NewGate(inflight),
		jobs:  map[string]*job{},
		loc:   time.UTC,
		obs:   observability.Nop(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// WithObserver gives the service an observer for its metrics, traces and logs.
// Without one it observes nothing. Running a job is observed by the queue
// (queue/run), so cron does not count the same run a second time.
func WithObserver(o observability.IObserver) Option {
	return func(c *CronService) { c.obs = observability.Pick(o) }
}

// begin starts the Operation of one unit of work. A service built as a bare
// struct literal has no observer, so it falls back to Nop without writing.
func (c *CronService) begin(ctx context.Context, name string) (context.Context, observability.Operation) {
	if c.obs == nil {
		return observability.Nop().Begin(ctx, observability.ComponentCron, name)
	}
	return c.obs.Begin(ctx, observability.ComponentCron, name)
}

func (c *CronService) Start(ctx context.Context) error {
	if c.gate == nil {
		return ErrNoTracker
	}
	if utils.IsNil(c.queue) {
		return ErrNoQueue
	}
	if err := c.queue.Ping(ctx); err != nil {
		return errors.Join(ErrQueueDown, err)
	}
	c.gate.Open()
	return nil
}

func (c *CronService) Run(ctx context.Context) error { return c.Start(ctx) }

func (c *CronService) Stop(ctx context.Context) error { return c.gate.Close(ctx) }

func (c *CronService) Ping(ctx context.Context) error {
	done, ok := c.gate.Enter()
	if !ok {
		return ErrNotStarted
	}
	defer done()
	return c.queue.Ping(ctx)
}

func (c *CronService) CreateJob(data CronJob) (err error) {
	if c.gate == nil || c.jobs == nil {
		return ErrNoTracker
	}
	if utils.IsNil(c.queue) {
		return ErrNoQueue
	}
	_, op := c.begin(context.Background(), "create_job")
	defer func() { op.End(err) }()
	var errs []error
	if strings.TrimSpace(data.JobID) == "" {
		errs = append(errs, ErrNoJobID)
	}
	if strings.TrimSpace(data.TaskName) == "" {
		errs = append(errs, ErrNoTaskName)
	}
	if data.Handler == nil {
		errs = append(errs, ErrNoHandler)
	}
	if err := validSchedule(data.Schedule); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.jobs[data.JobID]; ok {
		return ErrJobExists
	}
	for _, j := range c.jobs {
		if j.TaskName == data.TaskName {
			return ErrTaskTaken
		}
	}
	j := &job{CronJob: data}
	j.Payload = append([]byte(nil), data.Payload...) // the caller may reuse its slice
	handler := data.Handler
	c.queue.Handle(j.TaskName, func(ctx context.Context, payload []byte) (err error) {
		defer func() { // a panicking job fails this run, not the worker
			if r := recover(); r != nil {
				err = fmt.Errorf("%w: job %q: %v", ErrJobPanicked, data.JobID, r)
			}
		}()
		return handler(ctx, payload)
	}, queue.Guard(c.gate))
	if err := c.schedule(j); err != nil {
		return err
	}
	c.jobs[j.JobID] = j
	return nil
}

// validSchedule parses spec as the scheduler will. The time zone comes from
// Loc, not the spec, and "@every" must be at least a second: the parser
// silently turns anything shorter, even a negative duration, into every
// second, so a typo like "@every -1h" would run a job every second.
func validSchedule(spec string) error {
	spec = strings.TrimSpace(spec)
	invalid := func(why string) error { return fmt.Errorf("%w %q: %s", ErrInvalidSchedule, spec, why) }
	if strings.HasPrefix(spec, "CRON_TZ=") || strings.HasPrefix(spec, "TZ=") {
		return invalid("set the time zone with Loc")
	}
	if every, ok := strings.CutPrefix(spec, "@every "); ok {
		if d, err := time.ParseDuration(strings.TrimSpace(every)); err != nil || d < time.Second {
			return invalid("@every needs a duration of at least 1s")
		}
	}
	if _, err := parser.Parse(spec); err != nil {
		return invalid(err.Error())
	}
	return nil
}

// schedule registers j with the queue in its time zone; the caller holds mu.
func (c *CronService) schedule(j *job) error {
	loc := j.Loc
	if loc == nil {
		loc = c.loc
	}
	id, err := c.queue.Schedule("CRON_TZ="+loc.String()+" "+j.Schedule, j.TaskName, j.Payload)
	if err != nil {
		return errors.Join(ErrInvalidSchedule, err)
	}
	j.schedule = id
	return nil
}

func (c *CronService) RemoveJob(id string) (err error) {
	if c.jobs == nil {
		return ErrJobNotFound
	}
	_, op := c.begin(context.Background(), "remove_job")
	defer func() { op.End(err) }()
	c.mu.Lock()
	defer c.mu.Unlock()
	j, ok := c.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	delete(c.jobs, id)
	if err := c.queue.Unschedule(j.schedule); err != nil {
		return fmt.Errorf("cron: removing job %q: %w", id, err)
	}
	return nil
}

func (c *CronService) ExecuteJob(ctx context.Context, taskName string, payload []byte) error {
	c.mu.Lock()
	var found *job
	for _, j := range c.jobs {
		if j.TaskName == taskName {
			found = j
		}
	}
	c.mu.Unlock()
	if found == nil {
		return ErrUnknownTask
	}
	return c.execute(ctx, found, payload)
}

func (c *CronService) ExecuteJobByID(ctx context.Context, id string, payload []byte) error {
	c.mu.Lock()
	j := c.jobs[id]
	c.mu.Unlock()
	if j == nil {
		return ErrJobNotFound
	}
	return c.execute(ctx, j, payload)
}

func (c *CronService) execute(ctx context.Context, j *job, payload []byte) (err error) {
	done, ok := c.gate.Enter()
	if !ok {
		return ErrNotStarted
	}
	defer done()
	ctx, op := c.begin(ctx, "execute")
	defer func() { op.End(err) }()
	if payload == nil {
		payload = j.Payload
	}
	if _, err := c.queue.Enqueue(ctx, j.TaskName, payload); err != nil {
		return fmt.Errorf("cron: running job %q: %w", j.JobID, err)
	}
	return nil
}

func (c *CronService) SetLocation(loc *time.Location) error {
	if loc == nil {
		return ErrNilLocation
	}
	if c.jobs == nil {
		return ErrNoTracker
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loc = loc
	var errs []error
	for _, j := range c.jobs {
		if j.Loc != nil {
			continue
		}
		if err := c.queue.Unschedule(j.schedule); err != nil {
			errs = append(errs, err)
		}
		errs = append(errs, c.schedule(j))
	}
	return errors.Join(errs...)
}
