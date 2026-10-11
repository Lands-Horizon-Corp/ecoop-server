-- v043, phase 2. Running balance after each transaction, for statements.
-- +goose Up
ALTER TABLE transactions ADD COLUMN balance_after_cents bigint;

-- +goose Down
ALTER TABLE transactions DROP COLUMN balance_after_cents;
