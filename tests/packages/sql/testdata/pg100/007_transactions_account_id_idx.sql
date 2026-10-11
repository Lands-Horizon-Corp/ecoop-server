-- v007, phase 1. Statement pages read transactions by account.
-- +goose Up
CREATE INDEX transactions_account_id_idx ON transactions (account_id);

-- +goose Down
DROP INDEX transactions_account_id_idx;
