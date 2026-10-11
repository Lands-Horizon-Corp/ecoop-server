-- v017, phase 1. When the account was opened, backfilled from created_at for existing rows.
-- +goose Up
ALTER TABLE accounts ADD COLUMN opened_at timestamptz;
UPDATE accounts SET opened_at = created_at;

-- +goose Down
ALTER TABLE accounts DROP COLUMN opened_at;
