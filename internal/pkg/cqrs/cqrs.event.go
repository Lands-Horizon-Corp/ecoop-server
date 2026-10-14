package cqrs

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnCreated(ctx context.Context, data *TData) {
	if s == nil {
		return
	}
	s.handleEvent(ctx, cdc.OpCreate, data, s.Created)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnUpdated(ctx context.Context, data *TData) {
	if s == nil {
		return
	}
	s.handleEvent(ctx, cdc.OpUpdate, data, s.Updated)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnDeleted(ctx context.Context, data *TData) {
	if s == nil {
		return
	}
	s.handleEvent(ctx, cdc.OpDelete, data, s.Deleted)
}

// fireHooks runs the change hook of each change a committed batch made. The
// hooks are notifications (a websocket broadcast, say), not data: they run after
// the data is durable, never inside the transaction, and a crash right after the
// commit can lose them. A snapshot read (op "r") is replication catching up, not
// a change anyone is waiting to hear about, so it fires nothing.
func (c *CQRSService[TData, TResponse, TRequest, TID]) fireHooks(ctx context.Context, kept []*change[TData]) {
	for _, ch := range kept {
		switch ch.op {
		case cdc.OpCreate:
			c.OnCreated(ctx, &ch.data)
		case cdc.OpUpdate:
			c.OnUpdated(ctx, &ch.data)
		case cdc.OpDelete:
			c.OnDeleted(ctx, &ch.data)
		}
	}
}

// handleEvent runs one hook in the background, bounded. At most HookWorkers run
// at once; past that a hook is dropped (and counted) rather than queued without
// limit or allowed to hold up replication. Stop waits for the hooks running.
func (c *CQRSService[TData, TResponse, TRequest, TID]) handleEvent(
	ctx context.Context,
	eventType cdc.ChangeType,
	data *TData,
	getEvents func(*TData) []string,
) {
	if c == nil || c.st == nil || data == nil || c.ToResource == nil {
		return
	}
	select {
	case c.st.hookSlots <- struct{}{}:
	default:
		c.st.stats.hooksDropped.Add(1)
		c.log.Warn("change hook dropped: too many are running", "op", string(eventType), "limit", cap(c.st.hookSlots))
		return
	}
	done := c.st.hooks.Track()
	c.st.stats.hooks.Add(1)
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hookTimeout)
	row := *data // the hook runs after the caller moves on: it gets its own copy
	go func() {
		defer done()
		defer func() { <-c.st.hookSlots }()
		defer cancel()
		defer func() {
			if p := recover(); p != nil {
				c.log.Error("change hook panicked", "op", string(eventType), "panic", fmt.Sprint(p))
			}
		}()
		c.runHook(hctx, eventType, &row, getEvents)
	}()
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) runHook(
	ctx context.Context, eventType cdc.ChangeType, data *TData, getEvents func(*TData) []string,
) {
	payload := c.ToResource(data)
	if payload == nil {
		return
	}
	var events []string
	if getEvents != nil {
		events = getEvents(data)
	}
	if len(events) == 0 {
		return
	}
	if c.Dispatch != nil {
		if err := c.Dispatch(c.Channel, events, payload); err != nil {
			c.log.Error("change hook dispatch failed", "op", string(eventType), "error", err)
		}
	}
	if c.BroadcastService != nil {
		if err := c.BroadcastService.Broadcast(ctx, []string{c.Channel}, events, payload); err != nil {
			c.log.Error("change hook broadcast failed", "op", string(eventType), "error", err)
		}
	}
}
