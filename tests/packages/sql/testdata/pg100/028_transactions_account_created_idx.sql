-- v028, phase 2. A composite index serves 'latest transactions of an account' in one scan and
-- makes the single-column index redundant.
-- +goose Up
CREATE INDEX transactions_account_created_idx ON transactions (account_id, created_at DESC);
DROP INDEX transactions_account_id_idx;

-- +goose Down
CREATE INDEX transactions_account_id_idx ON transactions (account_id);
DROP INDEX transactions_account_created_idx;
