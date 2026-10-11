-- v062, phase 3. Expression index on the UTC day (immutable, so indexable) for daily totals.
-- +goose Up
CREATE INDEX transactions_day_idx ON transactions (date_trunc('day', created_at AT TIME ZONE 'UTC'));

-- +goose Down
DROP INDEX transactions_day_idx;
