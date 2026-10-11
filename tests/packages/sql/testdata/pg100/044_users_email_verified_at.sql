-- v044, phase 2. Email verification.
-- +goose Up
ALTER TABLE users ADD COLUMN email_verified_at timestamptz;

-- +goose Down
ALTER TABLE users DROP COLUMN email_verified_at;
