-- v029, phase 2. Audit column. now() is stable within a statement, so the default is still
-- catalog-only.
-- +goose Up
ALTER TABLE users ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();

-- +goose Down
ALTER TABLE users DROP COLUMN updated_at;
