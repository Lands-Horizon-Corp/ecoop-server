-- v006, phase 1. Postgres does not index foreign keys itself; without this, deleting a user scans
-- accounts.
-- +goose Up
CREATE INDEX accounts_user_id_idx ON accounts (user_id);

-- +goose Down
DROP INDEX accounts_user_id_idx;
