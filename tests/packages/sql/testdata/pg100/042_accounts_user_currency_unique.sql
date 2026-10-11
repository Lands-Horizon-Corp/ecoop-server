-- v042, phase 2. One account per user per currency (composite unique).
-- +goose Up
CREATE UNIQUE INDEX accounts_user_currency_key ON accounts (user_id, currency);

-- +goose Down
DROP INDEX accounts_user_currency_key;
