-- v012, phase 1. Reports filter by date.
-- +goose Up
CREATE INDEX transactions_created_at_idx ON transactions (created_at);

-- +goose Down
DROP INDEX transactions_created_at_idx;
