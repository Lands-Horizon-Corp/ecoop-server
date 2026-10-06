package cqrs

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broadcast"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) OnCreated(ctx context.Context, data *TData) {
	c.handleEvent(ctx, ChangeTypeCreated, data, c.Created)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) OnUpdated(ctx context.Context, data *TData) {
	c.handleEvent(ctx, ChangeTypeUpdated, data, c.Updated)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) OnDeleted(ctx context.Context, data *TData) {
	c.handleEvent(ctx, ChangeTypeDeleted, data, c.Deleted)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) handleEvent(
	ctx context.Context,
	eventType ChangeType,
	data *TData,
	getEvents func(*TData) broadcast.Events,
) {
	if data == nil || c.ToResource == nil {
		return
	}
	ctx = context.WithoutCancel(ctx)
	job := func() {
		defer func() {
			if re := recover(); re != nil {
				c.error(ctx, fmt.Errorf("panic: %v", re), "event handler panicked", "event_type", eventType.String())
			}
		}()
		payload := c.ToResource(data)
		if payload == nil {
			return
		}
		var events broadcast.Events
		if getEvents != nil {
			events = getEvents(data)
		}
		if len(events) == 0 {
			return
		}
		if c.Dispatch != nil {
			if err := c.Dispatch(c.Channel, events, payload); err != nil {
				c.error(ctx, err, "event dispatch failed", "event_type", eventType.String(), "events", events)
			}
		}
		if c.BroadcastService != nil {
			if err := c.BroadcastService.Broadcast([]broadcast.Channel{c.Channel}, events, payload); err != nil {
				c.error(ctx, err, "event broadcast failed", "event_type", eventType.String(), "events", events)
			}
		}
	}
	if !c.hooks.submit(job) {
		c.warn(ctx, "change hook dropped: service is shutting down", "event_type", eventType.String())
	}
}
