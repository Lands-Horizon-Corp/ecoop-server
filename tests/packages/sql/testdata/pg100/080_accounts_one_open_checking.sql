-- v080, phase 3. Partial unique index: at most one open checking account per user.
-- +goose Up
CREATE UNIQUE INDEX accounts_one_open_checking_key ON accounts (user_id) WHERE type = 'checking' AND status = 'open';

-- +goose Down
DROP INDEX accounts_one_open_checking_key;
