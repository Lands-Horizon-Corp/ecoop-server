-- v059, phase 3. Covering index (INCLUDE): statement pages read amounts from the index alone
-- (index-only scan).
-- +goose Up
CREATE INDEX transactions_account_created_cover_idx ON transactions (account_id, created_at DESC) INCLUDE (amount_cents);
DROP INDEX transactions_account_created_idx;

-- +goose Down
CREATE INDEX transactions_account_created_idx ON transactions (account_id, created_at DESC);
DROP INDEX transactions_account_created_cover_idx;
