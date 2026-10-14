package cqrs

import (
	"context"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnCreated(ctx context.Context, data *TData) {
	s.handleEvent(ctx, cdc.OpCreate, data, s.Created)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnUpdated(ctx context.Context, data *TData) {
	s.handleEvent(ctx, cdc.OpUpdate, data, s.Updated)
}

func (s *CQRSService[TData, TResponse, TRequest, TID]) OnDeleted(ctx context.Context, data *TData) {
	s.handleEvent(ctx, cdc.OpDelete, data, s.Deleted)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) handleEvent(
	ctx context.Context,
	eventType cdc.ChangeType,
	data *TData,
	getEvents func(*TData) []string,
) {
	if data == nil || c.ToResource == nil {
		return
	}
	asyncCtx := context.WithoutCancel(ctx)
	go func(ctx context.Context, data *TData) {
		defer func() {
			if re := recover(); re != nil {
				// c.error(ctx, fmt.Sprintf("%d panic on channel %s: %v", eventType, c.Channel, re))
				fmt.Printf("%s panic on channel %s: %v\n", eventType, c.Channel, re)
			}
		}()
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
				// c.error(ctx, fmt.Sprintf("%d dispatch failed [channel: %s]: %v (type: %T)", eventType, c.Channel, err, data))
				fmt.Printf("%s dispatch failed [channel: %s]: %v (type: %T)\n", eventType, c.Channel, err, data)
			}
		}
		if c.BroadcastService != nil {
			if err := c.BroadcastService.Broadcast(ctx, []string{c.Channel}, events, payload); err != nil {
				// c.error(ctx, fmt.Sprintf("%d broadcast failed [channel: %s]: %v (type: %T)", eventType, c.Channel, err, data))
				fmt.Printf("%s broadcast failed [channel: %s]: %v (type: %T)\n", eventType, c.Channel, err, data)
			}
		}
	}(asyncCtx, data)
}
