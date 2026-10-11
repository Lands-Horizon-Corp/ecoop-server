-- v070, phase 3. Partial index on debits only: fraud rules scan outgoing money, a fraction of all
-- rows.
-- +goose Up
CREATE INDEX transactions_debits_idx ON transactions (account_id) WHERE amount_cents < 0;

-- +goose Down
DROP INDEX transactions_debits_idx;
