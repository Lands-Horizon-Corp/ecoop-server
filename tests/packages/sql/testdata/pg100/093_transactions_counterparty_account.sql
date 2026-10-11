-- v093, phase 4. A structured counterparty, step 1: the column (propagates to every partition,
-- catalog-only).
-- +goose Up
SET LOCAL lock_timeout = '5s';
ALTER TABLE transactions ADD COLUMN counterparty_account_id bigint;

-- +goose Down
ALTER TABLE transactions DROP COLUMN counterparty_account_id;
