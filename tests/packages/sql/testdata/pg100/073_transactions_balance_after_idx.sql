-- v073, phase 3. Partial covering index: statement lines with a running balance, read from the
-- index alone.
-- +goose Up
CREATE INDEX transactions_balance_after_idx ON transactions (account_id) INCLUDE (balance_after_cents) WHERE balance_after_cents IS NOT NULL;

-- +goose Down
DROP INDEX transactions_balance_after_idx;
