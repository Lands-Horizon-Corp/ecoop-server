-- v014, phase 1. Session tracking.
-- +goose Up
ALTER TABLE users ADD COLUMN last_login_at timestamptz;

-- +goose Down
ALTER TABLE users DROP COLUMN last_login_at;
