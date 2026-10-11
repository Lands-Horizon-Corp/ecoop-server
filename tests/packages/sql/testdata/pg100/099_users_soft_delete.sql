-- v099, phase 4. Soft delete: catalog-only column; rows are hidden, never lost.
-- +goose Up
SET LOCAL lock_timeout = '5s';
ALTER TABLE users ADD COLUMN deleted_at timestamptz;

-- +goose Down
ALTER TABLE users DROP COLUMN deleted_at;
