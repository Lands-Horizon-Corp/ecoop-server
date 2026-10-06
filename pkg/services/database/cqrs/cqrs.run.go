package cqrs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/utils"
	"github.com/bytedance/sonic"
	"github.com/uptrace/bun"
)

// Run applies the change stream of this model's channel to the read database until ctx ends.
//
// With a broker that supports batch subscriptions (broker.BatchBrokerServices, e.g. Kafka) delivery
// is at-least-once: a batch is acknowledged only after it is applied, transient failures are retried
// in place (blocking the partition rather than skipping data), and messages that can never be applied
// go to the dead-letter topic before being acknowledged. Redeliveries are made harmless by the
// processed_events table. A plain broker acknowledges on receipt, so delivery is at-most-once.
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
	if bb, ok := c.MessageBrokerService.(broker.BatchBrokerServices); ok {
		c.info(ctx, "outbox runner started", "delivery", "at-least-once", "dlq", c.DLQTopic)
		return bb.SubscribeBatch(ctx, string(c.Channel), func(msgs []broker.Message) error {
			return c.handleDelivery(ctx, msgs)
		})
	}

	c.info(ctx, "outbox runner started", "batch_size", c.BatchSize, "flush_interval", c.FlushInterval.String())
	c.warn(ctx, "broker acknowledges on receipt; outbox delivery is at-most-once", "channel", string(c.Channel))
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
		env, err := c.decodeLegacy(ctx, key, value)
		if err != nil {
			c.error(ctx, err, "outbox payload unmarshal failed", "key", string(key), "bytes", len(value))
			_ = c.deadLetter(ctx, key, value, err)
			return nil
		}
		return batcher.Push(ctx, env)
	})
}

// decodeLegacy is decodeMessage for the at-most-once path, which also accepts a message with neither
// an event id nor a key by synthesizing an id (such a message cannot be deduplicated).
func (c *CQRSService[TData, TResponse, TRequest, TID]) decodeLegacy(ctx context.Context, key, value []byte) (CQRSQueuePayload[TData], error) {
	env, err := c.decodeMessage(key, value)
	if err == nil || !errors.Is(err, ErrMalformedMessage) || len(key) > 0 {
		return env, err
	}
	var bare CQRSQueuePayload[TData]
	if sonic.Unmarshal(value, &bare) != nil || bare.EventID != "" {
		return env, err
	}
	bare.EventID = fmt.Sprintf("%s-%d", c.Channel, time.Now().UnixNano())
	c.warn(ctx, "outbox message has no event id and no key; synthesized one, check the producer", "synthesized_event_id", bare.EventID)
	return bare, nil
}

// handleDelivery applies one broker batch. It returns nil only once every message is either applied
// or dead-lettered, which is when the broker may commit the batch.
func (c *CQRSService[TData, TResponse, TRequest, TID]) handleDelivery(ctx context.Context, msgs []broker.Message) error {
	batch := make([]CQRSQueuePayload[TData], 0, len(msgs))
	for _, m := range msgs {
		env, err := c.decodeMessage(m.Key, m.Value)
		if err != nil {
			c.error(ctx, err, "outbox payload unmarshal failed", "key", string(m.Key), "bytes", len(m.Value), "offset", m.Offset)
			if err := c.deadLetter(ctx, m.Key, m.Value, err); err != nil {
				return err // keep the batch unacknowledged rather than lose the message
			}
			continue
		}
		batch = append(batch, env)
	}
	return c.applyWithRetry(ctx, batch)
}

func (c *CQRSService[TData, TResponse, TRequest, TID]) applyWithRetry(ctx context.Context, batch []CQRSQueuePayload[TData]) error {
	backoff := c.RetryBackoff
	for attempt := 1; ; attempt++ {
		started := time.Now()
		applied, rejected, err := c.applyOutcome(ctx, batch)
		c.afterApply(ctx, len(batch), applied, started)
		if err == nil {
			for _, r := range rejected {
				value, _ := sonic.Marshal(r.msg)
				if dlqErr := c.deadLetter(ctx, []byte(r.msg.EventID), value, r.err); dlqErr != nil {
					return dlqErr
				}
			}
			return nil
		}
		if c.MaxRetries > 0 && attempt >= c.MaxRetries {
			c.error(ctx, err, "outbox batch still failing, stopping without acknowledging it", "attempts", attempt, "batch_size", len(batch))
			return err
		}
		c.warn(ctx, "outbox batch failed transiently, retrying", "attempt", attempt, "backoff", backoff.String(), "error", err.Error())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

type rejectedMessage[TData any] struct {
	msg CQRSQueuePayload[TData]
	err error
}

// applyOutcome applies batch to the read database. Messages the read model permanently rejects are
// returned in rejected; a transient failure is returned as err (the caller retries; messages already
// applied are recorded in processed_events, so the retry skips them).
func (c *CQRSService[TData, TResponse, TRequest, TID]) applyOutcome(
	ctx context.Context,
	batch []CQRSQueuePayload[TData],
) (applied []CQRSQueuePayload[TData], rejected []rejectedMessage[TData], err error) {
	if len(batch) == 0 {
		return nil, nil, nil
	}
	applied, err = c.syncBatchToReadDB(ctx, batch)
	switch {
	case err == nil:
		return applied, nil, nil
	case isTransient(err):
		return nil, nil, err
	case len(batch) == 1:
		return nil, []rejectedMessage[TData]{{batch[0], err}}, nil
	}
	// One bad message fails the whole batch transaction: apply them one by one so only it fails.
	c.warn(ctx, "outbox batch failed, retrying messages individually", "batch_size", len(batch), "error", err.Error())
	for i := range batch {
		ok, err := c.syncBatchToReadDB(ctx, batch[i:i+1])
		switch {
		case err == nil:
			applied = append(applied, ok...)
		case isTransient(err):
			return applied, rejected, err
		default:
			c.error(ctx, err, "outbox message rejected by read db", "event_id", batch[i].EventID)
			rejected = append(rejected, rejectedMessage[TData]{batch[i], err})
		}
	}
	return applied, rejected, nil
}

// processBatch is the at-most-once path's batch handler.
func (c *CQRSService[TData, TResponse, TRequest, TID]) processBatch(
	ctx context.Context,
	batch []CQRSQueuePayload[TData],
) error {
	started := time.Now()
	applied, rejected, err := c.applyOutcome(ctx, batch)
	c.afterApply(ctx, len(batch), applied, started)
	for _, r := range rejected {
		value, _ := sonic.Marshal(r.msg)
		_ = c.deadLetter(ctx, []byte(r.msg.EventID), value, r.err)
	}
	if err != nil {
		return fmt.Errorf("synchronizing batch to read db: %w", err)
	}
	if len(rejected) > 0 && len(applied) == 0 {
		return fmt.Errorf("synchronizing batch to read db: %w", rejected[0].err)
	}
	return nil
}

// afterApply logs the sync summary and fires the change hooks for the applied messages.
func (c *CQRSService[TData, TResponse, TRequest, TID]) afterApply(
	ctx context.Context, received int, applied []CQRSQueuePayload[TData], started time.Time,
) {
	if len(applied) == 0 {
		return
	}
	c.success(ctx, "read db synchronized",
		"received", received, "applied", len(applied), "duration_ms", time.Since(started).Milliseconds())
	for i := range applied {
		msg := &applied[i]
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
}

type deadLetterRecord struct {
	Channel  string          `json:"channel"`
	Error    string          `json:"error"`
	Original json.RawMessage `json:"original,omitempty"`
	Raw      []byte          `json:"raw,omitempty"` // base64 when the original is not JSON
	FailedAt time.Time       `json:"failed_at"`
}

// deadLetter keeps a message that can never be applied on the dead-letter topic.
func (c *CQRSService[TData, TResponse, TRequest, TID]) deadLetter(ctx context.Context, key, value []byte, cause error) error {
	if c.DLQTopic == "-" {
		c.warn(ctx, "outbox message dropped (dead-letter topic disabled)", "key", string(key))
		return nil
	}
	rec := deadLetterRecord{Channel: string(c.Channel), Error: cause.Error(), FailedAt: time.Now().UTC()}
	if json.Valid(value) {
		rec.Original = value
	} else {
		rec.Raw = value
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := c.MessageBrokerService.Publish(ctx, c.DLQTopic, key, body); err != nil {
		c.error(ctx, err, "outbox dead-letter publish failed", "topic", c.DLQTopic, "key", string(key))
		return err
	}
	c.warn(ctx, "outbox message dead-lettered", "topic", c.DLQTopic, "key", string(key), "cause", cause.Error())
	return nil
}

// isTransient reports whether a failed batch may succeed on retry (see sql.IsTransient).
func isTransient(err error) bool {
	return errors.Is(err, ErrReadDBNotInitialized) || sqlsvc.IsTransient(err)
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
	// Several replicas may apply overlapping batches at once. Writing rows in one fixed order (by event
	// id and by entity key) makes them take row locks in the same order, so they never deadlock.
	slices.SortFunc(eventsToInsert, func(a, b ProcessedEvent) int { return strings.Compare(a.EventID, b.EventID) })
	slices.Sort(entityOrder)
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
		// The read model mirrors every tenant: row-level security lets the replicator through.
		if err := sqlsvc.SetReplicator(ctx, tx); err != nil {
			return err
		}
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
