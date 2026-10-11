-- v056, phase 3. Rail-specific transaction details as JSONB.
-- +goose Up
ALTER TABLE transactions ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}'::jsonb;

-- +goose Down
ALTER TABLE transactions DROP COLUMN metadata;
