-- v030, phase 2. Audit column.
-- +goose Up
ALTER TABLE accounts ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();

-- +goose Down
ALTER TABLE accounts DROP COLUMN updated_at;
