-- v048, phase 2. Idempotency: a reference posts once per account. The plain reference index is
-- superseded.
-- +goose Up
CREATE UNIQUE INDEX transactions_account_reference_key ON transactions (account_id, reference);
DROP INDEX transactions_reference_idx;

-- +goose Down
CREATE INDEX transactions_reference_idx ON transactions (reference);
DROP INDEX transactions_account_reference_key;
