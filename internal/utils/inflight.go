package utils

import (
	"context"
	"sync"
	"sync/atomic"
)

// Inflight counts operations in progress so a service can wait for them to
// finish on shutdown.
//
// Track is a single atomic add and done only takes the lock when the count
// reaches zero. The idle channel exists only while someone is waiting:
// Wait creates it under mu, and whichever done brings the count to zero
// closes it under mu, so a waiter can never miss the transition.
type Inflight struct {
	count atomic.Int64
	mu    sync.Mutex
	idle  chan struct{}
}

func NewInflight() *Inflight {
	return &Inflight{}
}

func (f *Inflight) Track() (done func()) {
	f.count.Add(1)
	var released atomic.Bool
	return func() {
		if released.CompareAndSwap(false, true) {
			f.release()
		}
	}
}

func (f *Inflight) release() {
	if f.count.Add(-1) != 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.count.Load() == 0 && f.idle != nil {
		close(f.idle)
		f.idle = nil
	}
}

func (f *Inflight) Wait(ctx context.Context) error {
	f.mu.Lock()
	if f.count.Load() == 0 {
		f.mu.Unlock()
		return nil
	}
	if f.idle == nil {
		f.idle = make(chan struct{})
	}
	idle := f.idle
	f.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *Inflight) Count() int {
	return int(f.count.Load())
}
