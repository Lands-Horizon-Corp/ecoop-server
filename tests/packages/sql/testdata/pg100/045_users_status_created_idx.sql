-- v045, phase 2. Back-office lists users by state, newest first.
-- +goose Up
CREATE INDEX users_status_created_idx ON users (status, created_at);

-- +goose Down
DROP INDEX users_status_created_idx;
