-- v069, phase 3. Partitions are created ahead of time, never on demand at midnight.
-- +goose Up
CREATE TABLE transactions_y2028 PARTITION OF transactions FOR VALUES FROM ('2028-01-01 00:00:00+00') TO ('2029-01-01 00:00:00+00');

-- +goose Down
DROP TABLE transactions_y2028;
