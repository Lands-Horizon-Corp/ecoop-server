package cqrs

import (
	"context"
	"sync"
)

// hookPool runs the change hooks (Dispatch and Broadcast) on a fixed number of workers. A burst of
// changes queues behind them instead of starting one goroutine per change, and a full queue makes
// the caller wait (backpressure) rather than growing without bound. Shutdown drains it.
type hookPool struct {
	workers int
	once    sync.Once
	mu      sync.RWMutex // held for reading while submitting, for writing while closing
	closed  bool
	queue   chan func()
	wg      sync.WaitGroup
}

func newHookPool(workers int) *hookPool {
	if workers <= 0 {
		workers = 64
	}
	return &hookPool{workers: workers}
}

func (p *hookPool) start() {
	p.once.Do(func() {
		p.queue = make(chan func(), p.workers*4)
		for range p.workers {
			p.wg.Go(func() {
				for job := range p.queue {
					job()
				}
			})
		}
	})
}

// submit queues job, waiting while the queue is full. It reports false once the pool is shut down.
func (p *hookPool) submit(job func()) bool {
	p.start()
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return false
	}
	p.queue <- job
	return true
}

// shutdown stops accepting jobs and waits for the queued ones, or for ctx.
func (p *hookPool) shutdown(ctx context.Context) error {
	p.start()
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown drains the change hooks still queued. Hooks for changes after Shutdown are dropped.
func (c *CQRSService[TData, TResponse, TRequest, TID]) Shutdown(ctx context.Context) error {
	return c.hooks.shutdown(ctx)
}
