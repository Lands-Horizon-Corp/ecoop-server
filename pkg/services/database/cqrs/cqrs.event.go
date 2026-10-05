package cqrs

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) OnCreated(ctx context.Context, data *TData) {
	c.handleEvent(ctx, database.ChangeTypeCreated, data, c.Created)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) OnUpdated(ctx context.Context, data *TData) {
	c.handleEvent(ctx, database.ChangeTypeUpdated, data, c.Updated)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) OnDeleted(ctx context.Context, data *TData) {
	c.handleEvent(ctx, database.ChangeTypeDeleted, data, c.Deleted)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) handleEvent(
	ctx context.Context,
	eventType database.ChangeType,
	data *TData,
	getEvents func(*TData) database.Events,
) {
	if data == nil || c.ToResource == nil {
		return
	}
	asyncCtx := context.WithoutCancel(ctx)
	go func(ctx context.Context, data *TData) {
		defer func() {
			if re := recover(); re != nil {
				c.error(ctx, fmt.Sprintf("%d panic on channel %s: %v", eventType, c.Channel, re))
			}
		}()
		payload := c.ToResource(data)
		if payload == nil {
			return
		}
		var events database.Events
		if getEvents != nil {
			events = getEvents(data)
		}
		if len(events) == 0 {
			return
		}
		if c.Dispatch != nil {
			if err := c.Dispatch(c.Channel, events, payload); err != nil {
				c.error(ctx, fmt.Sprintf("%d dispatch failed [channel: %s]: %v (type: %T)", eventType, c.Channel, err, data))
			}
		}
		if c.BroadcastService != nil {
			if err := c.BroadcastService.Broadcast([]database.Channel{c.Channel}, events, payload); err != nil {
				c.error(ctx, fmt.Sprintf("%d broadcast failed [channel: %s]: %v (type: %T)", eventType, c.Channel, err, data))
			}
		}
	}(asyncCtx, data)
}
