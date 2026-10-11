package utils

import (
	"context"
	"sync/atomic"
)

// Gate is a service's running state together with its in-flight work: Open
// on Start, Close on Stop, Enter around every operation. Close refuses new
// work and waits for what already entered.
//
// All methods are safe on a nil *Gate: it behaves as a closed gate.
type Gate struct {
	open     atomic.Bool
	inflight *Inflight
}

// NewGate returns a closed gate tracking work on inflight, which may be
// shared with other services; nil gives the gate its own.
func NewGate(inflight *Inflight) *Gate {
	if inflight == nil {
		inflight = NewInflight()
	}
	return &Gate{inflight: inflight}
}

func (g *Gate) Open() {
	g.open.Store(true)
}

func (g *Gate) IsOpen() bool {
	return g != nil && g.open.Load()
}

// Close refuses new work and waits for work in progress, or returns
// ctx.Err() if ctx ends first. Closing a closed gate does nothing.
func (g *Gate) Close(ctx context.Context) error {
	if g == nil || !g.open.Swap(false) {
		return nil
	}
	return g.inflight.Wait(ctx)
}

// Enter registers one piece of work. ok is false when the gate is closed;
// otherwise call done when the work finishes. It registers before checking
// again, so a Close that begins at the same moment cannot miss the work.
func (g *Gate) Enter() (done func(), ok bool) {
	if !g.IsOpen() {
		return nil, false
	}
	done = g.inflight.Track()
	if !g.IsOpen() {
		done()
		return nil, false
	}
	return done, true
}
