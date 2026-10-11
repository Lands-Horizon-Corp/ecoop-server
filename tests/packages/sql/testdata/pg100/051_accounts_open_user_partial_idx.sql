-- v051, phase 3. Partial index: only open accounts are looked up by user, so closed ones stay out
-- of it.
-- +goose Up
CREATE INDEX accounts_open_user_idx ON accounts (user_id) WHERE status = 'open';

-- +goose Down
DROP INDEX accounts_open_user_idx;
