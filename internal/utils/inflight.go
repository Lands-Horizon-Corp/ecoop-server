package utils

import (
	"context"
	"sync"
)

type Inflight struct {
	mu    sync.Mutex
	count int
	idle  chan struct{}
}

func NewInflight() *Inflight {
	return &Inflight{}
}

func (f *Inflight) Track() (done func()) {
	f.mu.Lock()
	f.count++
	if f.idle == nil {
		f.idle = make(chan struct{})
	}
	f.mu.Unlock()
	return sync.OnceFunc(f.release)
}

func (f *Inflight) release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count--
	if f.count == 0 {
		close(f.idle)
		f.idle = nil
	}
}

func (f *Inflight) Wait(ctx context.Context) error {
	f.mu.Lock()
	idle := f.idle
	f.mu.Unlock()
	if idle == nil {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *Inflight) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}
