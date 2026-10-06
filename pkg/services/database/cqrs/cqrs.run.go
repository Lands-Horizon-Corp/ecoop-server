package cqrs

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/bytedance/sonic"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) Run(ctx context.Context) error {
	if c.WriteSQLService.Ping(ctx) != nil {
		panic("WriteSQLService is not reachable")
	}
	if c.ReadSQLService != nil {
		if c.ReadSQLService.Ping(ctx) != nil {
			panic("ReadSQLService is not reachable")
		}
	}
	if c.MessageBrokerService == nil {
		return ErrMessageBrokerNotInitialized
	}
	c.info(ctx, "outbox runner started", "batch_size", c.BatchSize, "flush_interval", c.FlushInterval.String())
	batcher := utils.NewBatcher(utils.BatcherConfig[CQRSQueuePayload[TData]]{
		BatchSize:     c.BatchSize,
		FlushInterval: c.FlushInterval,
		Handler: func(batchCtx context.Context, batch []CQRSQueuePayload[TData]) error {
			return c.processBatch(batchCtx, batch)
		},
		OnError: func(err error, batch []CQRSQueuePayload[TData]) {
			c.error(ctx, err, "outbox batch failed", "batch_size", len(batch))
		},
	})
	batcher.Start(ctx)
	defer batcher.Stop()

	return c.MessageBrokerService.Subscribe(ctx, string(c.Channel), func(key, value []byte) error {
		var env CQRSQueuePayload[TData]
		if err := sonic.Unmarshal(value, &env); err != nil {
			c.error(ctx, err, "outbox payload unmarshal failed", "key", string(key), "bytes", len(value))
			return nil
		}
		if env.EventID == "" {
			if len(key) > 0 {
				env.EventID = string(key)
			} else {
				env.EventID = fmt.Sprintf("%s-%d", c.Channel, time.Now().UnixNano())
				c.warn(ctx, "outbox message has no event id and no key; synthesized one, check the producer", "synthesized_event_id", env.EventID)
			}
		}
		return batcher.Push(ctx, env)
	})
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) processBatch(
	ctx context.Context,
	batch []CQRSQueuePayload[TData],
) error {
	if len(batch) == 0 {
		return nil
	}
	started := time.Now()
	appliedMessages, err := c.syncBatchToReadDB(ctx, batch)
	if err != nil {
		return fmt.Errorf("synchronizing batch to read db: %w", err)
	}
	if len(appliedMessages) > 0 {
		c.success(ctx, "read db synchronized",
			"received", len(batch), "applied", len(appliedMessages), "duration_ms", time.Since(started).Milliseconds())
	}
	for i := range appliedMessages {
		msg := &appliedMessages[i]
		switch msg.ChangeType {
		case ChangeTypeCreated:
			c.OnCreated(ctx, &msg.Payload)
		case ChangeTypeUpdated:
			c.OnUpdated(ctx, &msg.Payload)
		case ChangeTypeDeleted:
			c.OnDeleted(ctx, &msg.Payload)
		default:
			c.handleEvent(ctx, msg.ChangeType, &msg.Payload, nil)
		}
	}
	return nil
}

func (r *CQRSService[TData, TResponse, TRequest, TID]) syncBatchToReadDB(
	ctx context.Context,
	batch []CQRSQueuePayload[TData],
) ([]CQRSQueuePayload[TData], error) {
	if r.ReadSQLService == nil {
		return nil, ErrReadDBNotInitialized
	}
	eventIDsPtr := r.stringSlicePool.Get()
	defer r.stringSlicePool.Put(eventIDsPtr)

	seenInBatch := r.stringSetPool.Get()
	defer r.stringSetPool.Put(seenInBatch)

	existingIDsPtr := r.stringSlicePool.Get()
	defer r.stringSlicePool.Put(existingIDsPtr)

	existingMap := r.stringSetPool.Get()
	defer r.stringSetPool.Put(existingMap)

	eventsToInsertPtr := r.processedEventsPool.Get()
	defer r.processedEventsPool.Put(eventsToInsertPtr)

	eventIDs := *eventIDsPtr
	uniqueBatch := make([]CQRSQueuePayload[TData], 0, len(batch))

	for _, msg := range batch {
		if !seenInBatch[msg.EventID] {
			seenInBatch[msg.EventID] = true
			eventIDs = append(eventIDs, msg.EventID)
			uniqueBatch = append(uniqueBatch, msg)
		}
	}
	*eventIDsPtr = eventIDs

	if len(uniqueBatch) == 0 {
		return nil, nil
	}

	err := r.ReadSQLService.Client().NewSelect().
		Model((*ProcessedEvent)(nil)).
		Column("event_id").
		Where("event_id IN (?)", bun.List(*eventIDsPtr)).
		Scan(ctx, existingIDsPtr)
	if err != nil {
		return nil, fmt.Errorf("querying existing event ids: %w", err)
	}

	for _, id := range *existingIDsPtr {
		existingMap[id] = true
	}
	newMessages := make([]CQRSQueuePayload[TData], 0, len(uniqueBatch))
	eventsToInsert := *eventsToInsertPtr
	latestEntityState := make(map[string]CQRSQueuePayload[TData], len(uniqueBatch))
	entityOrder := make([]string, 0, len(uniqueBatch))
	now := time.Now()

	for _, msg := range uniqueBatch {
		if existingMap[msg.EventID] {
			continue
		}
		newMessages = append(newMessages, msg)
		eventsToInsert = append(eventsToInsert, ProcessedEvent{
			EventID:   msg.EventID,
			Channel:   string(r.Channel),
			CreatedAt: now,
		})
		key := utils.FieldValueAt(&msg.Payload, r.idFieldIndex)
		if key == "" {
			key = msg.EventID
		}
		if _, exists := latestEntityState[key]; !exists {
			entityOrder = append(entityOrder, key)
		}
		latestEntityState[key] = msg
	}
	*eventsToInsertPtr = eventsToInsert
	if len(newMessages) == 0 {
		return nil, nil
	}

	upsertEntities := make([]TData, 0, len(entityOrder))
	deleteEntities := make([]TData, 0, len(entityOrder))
	for _, key := range entityOrder {
		msg := latestEntityState[key]
		switch msg.ChangeType {
		case ChangeTypeCreated, ChangeTypeUpdated:
			upsertEntities = append(upsertEntities, msg.Payload)
		case ChangeTypeDeleted:
			deleteEntities = append(deleteEntities, msg.Payload)
		}
	}
	err = r.ReadSQLService.Client().RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().
			Model(eventsToInsertPtr).
			Ignore().
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("bulk inserting processed events: %w", err)
		}
		if len(upsertEntities) > 0 {
			_, err = tx.NewInsert().
				Model(&upsertEntities).
				On(fmt.Sprintf("CONFLICT (%s) DO UPDATE", r.ColumnDefaultID)).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("bulk upserting entities to read db: %w", err)
			}
		}
		if len(deleteEntities) > 0 {
			_, err = tx.NewDelete().
				Model(&deleteEntities).
				WherePK().
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("bulk deleting entities from read db: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return newMessages, nil
}
