package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/bytedance/sonic"
	"github.com/uptrace/bun"
)

func (c *CQRSService[TData, TResponse, TRequest, TID]) Run(ctx context.Context) error {
	if err := c.WriteSQLService.Ping(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrWriteDBUnreachable, err)
	}
	if c.ReadSQLService != nil {
		if err := c.ReadSQLService.Ping(ctx); err != nil {
			return fmt.Errorf("%w: %w", ErrReadDBUnreachable, err)
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
	if err != nil && len(batch) > 1 {
		// One bad message (e.g. a row the read model rejects) fails the whole batch transaction.
		// Apply the messages one by one so the valid ones still land and only the bad ones fail.
		c.warn(ctx, "outbox batch failed, retrying messages individually", "batch_size", len(batch), "error", err.Error())
		appliedMessages, err = c.syncOneByOne(ctx, batch)
	}
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
	if r.ReadSQLService == nil || r.ReadSQLService.Client() == nil {
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
		eventsToInsert = append(eventsToInsert, ProcessedEvent{
			EventID:   msg.EventID,
			Channel:   string(r.Channel),
			CreatedAt: now,
		})
		// A payload without an id (null, or the id field missing) cannot be applied: it would
		// upsert a row with an empty primary key. Record it as processed so it is not retried.
		if r.idFieldIndex >= 0 && reflect.ValueOf(&msg.Payload).Elem().Field(r.idFieldIndex).IsZero() {
			r.warn(ctx, "outbox message dropped: payload has no id", "event_id", msg.EventID, "change_type", msg.ChangeType.String())
			continue
		}
		newMessages = append(newMessages, msg)
		key := utils.FieldValueAt(&msg.Payload, r.idFieldIndex)
		if key == "" {
			key = msg.EventID
		}
		prev, exists := latestEntityState[key]
		if !exists {
			entityOrder = append(entityOrder, key)
		} else if r.versionNewer(&prev.Payload, &msg.Payload) {
			continue // an older version arrived after a newer one in the same batch
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
			q := tx.NewInsert().
				Model(&upsertEntities).
				On(fmt.Sprintf("CONFLICT (%s) DO UPDATE", r.ColumnDefaultID))
			if r.versionFieldIndex >= 0 {
				// Keep the stored row when it is newer than the incoming change.
				q = q.Where("?TableAlias.? <= EXCLUDED.?", bun.Ident(r.ColumnVersion), bun.Ident(r.ColumnVersion))
			}
			_, err = q.Exec(ctx)
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

// versionFieldIndex is the struct field index of the version column, or -1 when there is none or
// its type cannot be ordered.
func versionFieldIndex[TData any](column string) int {
	if column == "" {
		return -1
	}
	idx := utils.BunColumnFieldIndex[TData](column)
	if idx < 0 {
		return -1
	}
	if _, ok := versionKey(reflect.New(reflect.TypeFor[TData]()).Elem().Field(idx)); !ok {
		return -1
	}
	return idx
}

// versionNewer reports whether a carries a strictly newer version than b.
func (r *CQRSService[TData, TResponse, TRequest, TID]) versionNewer(a, b *TData) bool {
	if r.versionFieldIndex < 0 {
		return false
	}
	va, okA := versionKey(reflect.ValueOf(a).Elem().Field(r.versionFieldIndex))
	vb, okB := versionKey(reflect.ValueOf(b).Elem().Field(r.versionFieldIndex))
	return okA && okB && va > vb
}

// versionKey turns an integer or time version into an exactly comparable number.
func versionKey(v reflect.Value) (int64, bool) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, true
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return int64(v.Uint()), true
	}
	if t, ok := v.Interface().(time.Time); ok {
		return t.UnixNano(), true
	}
	return 0, false
}

// syncOneByOne applies each message in its own transaction, in arrival order. It returns the applied
// messages and an error joining every message that failed; failed messages are not recorded as
// processed, so a redelivery can apply them later.
func (c *CQRSService[TData, TResponse, TRequest, TID]) syncOneByOne(
	ctx context.Context,
	batch []CQRSQueuePayload[TData],
) ([]CQRSQueuePayload[TData], error) {
	var (
		applied []CQRSQueuePayload[TData]
		errs    []error
	)
	for i := range batch {
		ok, err := c.syncBatchToReadDB(ctx, batch[i:i+1])
		if err != nil {
			c.error(ctx, err, "outbox message rejected by read db", "event_id", batch[i].EventID)
			errs = append(errs, fmt.Errorf("event %s: %w", batch[i].EventID, err))
			continue
		}
		applied = append(applied, ok...)
	}
	if len(applied) == 0 {
		return nil, errors.Join(errs...)
	}
	return applied, nil
}
