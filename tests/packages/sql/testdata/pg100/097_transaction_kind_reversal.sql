-- v097, phase 4. Adding an enum value is catalog-only. Removing one is not: Down rebuilds the
-- type, which rewrites the table, so it belongs in a maintenance window.
-- +goose Up
SET LOCAL lock_timeout = '5s';
ALTER TYPE transaction_kind ADD VALUE IF NOT EXISTS 'reversal';

-- +goose Down
ALTER TYPE transaction_kind RENAME TO transaction_kind_old;
CREATE TYPE transaction_kind AS ENUM ('deposit', 'withdrawal', 'transfer_in', 'transfer_out', 'fee');
ALTER TABLE transactions ALTER COLUMN kind TYPE transaction_kind USING kind::text::transaction_kind;
DROP TYPE transaction_kind_old;
