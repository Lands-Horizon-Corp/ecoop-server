package cqrs

import (
	"context"
	"errors"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Run starts replicating the model's Debezium topic into the read database. It
// checks first that everything the replication needs is in place (see verify)
// and returns an error instead of starting half-ready. It does not block: the
// CDC service runs the consumer, and Stop ends it.
//
// Delivery is at-least-once and the effect is exactly-once. A batch of records
// is written to the read database together with a checkpoint of how far the
// partition has been applied, in one transaction; Kafka is told about the
// progress only afterwards. A crash, a failover or a rebalance in between
// replays records the checkpoint already covers, and they are skipped, so any
// number of replicas can share the topic: each applies a partition, and a
// replica that briefly overlaps another waits on the checkpoint row lock and
// then skips what the first one did.
func (c *CQRSService[TData, TResponse, TRequest, TID]) Run(ctx context.Context) error {
	if c == nil || c.st == nil || c.model == nil {
		return ErrNotConstructed
	}
	if ctx == nil {
		return ErrNilContext
	}
	if utils.IsNil(c.CDCService) {
		return ErrCDCNotInitialized
	}
	c.st.lifecycle.Lock()
	defer c.st.lifecycle.Unlock()
	if c.st.gate.IsOpen() {
		return nil
	}
	if err := c.verify(ctx); err != nil {
		return err
	}
	opts := cdc.BatchOptions{
		MaxBatch: c.BatchSize,
		OnAssigned: func(parts []int32) {
			c.log.Info("partitions assigned", "partitions", parts)
		},
		OnRevoked: func(parts []int32) {
			c.log.Info("partitions revoked", "partitions", parts)
		},
		OnCommitError: func(partition int32, offset int64, err error) {
			c.st.stats.commitFails.Add(1)
			c.log.Warn("kafka offset commit failed; the records will be delivered again and skipped",
				"partition", partition, "offset", offset, "error", err.Error())
		},
	}
	// The gate opens before subscribing: a batch can arrive the moment the
	// consumer starts, and handle refuses batches while the gate is shut.
	c.st.gate.Open()
	if err := c.CDCService.SubscribeBatch(ctx, c.topic, opts, c.handle); err != nil {
		_ = c.st.gate.Close(context.WithoutCancel(ctx))
		return fmt.Errorf("subscribing to %s: %w", c.topic, err)
	}
	c.startRepair(ctx)
	c.log.Info("replication started", "group", c.CDCService.GroupID(), "batch_size", c.BatchSize)
	return nil
}

// Stop ends replication. It stops the consumer of the topic first (the batch
// being applied is told to wind down; its transaction commits whole or rolls
// back whole and the records it did not finish are delivered again, to this
// or another replica), then waits for the change hooks that are running. ctx
// bounds the wait; on timeout the work still running is abandoned, not killed.
func (c *CQRSService[TData, TResponse, TRequest, TID]) Stop(ctx context.Context) error {
	if c == nil || c.st == nil {
		return nil
	}
	if ctx == nil {
		return ErrNilContext
	}
	c.st.lifecycle.Lock()
	defer c.st.lifecycle.Unlock()
	if !c.st.gate.IsOpen() {
		return nil
	}
	var errs []error
	if err := c.stopRepair(ctx); err != nil {
		errs = append(errs, fmt.Errorf("waiting for the repair pass: %w", err))
	}
	if err := c.CDCService.Unsubscribe(ctx, c.topic); err != nil {
		errs = append(errs, fmt.Errorf("unsubscribing from %s: %w", c.topic, err))
	}
	// A batch that outlived Unsubscribe still holds the gate; Close waits for it.
	if err := c.st.gate.Close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("waiting for the batch being applied: %w", err))
	}
	if err := c.st.hooks.Wait(ctx); err != nil {
		errs = append(errs, fmt.Errorf("waiting for change hooks: %w", err))
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	c.log.Info("replication stopped")
	return nil
}
