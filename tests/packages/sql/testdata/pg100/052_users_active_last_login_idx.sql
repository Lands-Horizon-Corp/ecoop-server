-- v052, phase 3. Partial index for the 'recently active' dashboard.
-- +goose Up
CREATE INDEX users_active_last_login_idx ON users (last_login_at DESC) WHERE status = 'active';

-- +goose Down
DROP INDEX users_active_last_login_idx;
