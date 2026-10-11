-- v033, phase 2. Optimistic locking for balances.
-- +goose Up
ALTER TABLE accounts ADD COLUMN version integer NOT NULL DEFAULT 1;

-- +goose Down
ALTER TABLE accounts DROP COLUMN version;
