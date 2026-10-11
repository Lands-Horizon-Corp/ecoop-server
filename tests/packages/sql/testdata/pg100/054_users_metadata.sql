-- v054, phase 3. Semi-structured attributes as JSONB.
-- +goose Up
ALTER TABLE users ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}'::jsonb;

-- +goose Down
ALTER TABLE users DROP COLUMN metadata;
