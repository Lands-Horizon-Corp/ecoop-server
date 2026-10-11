-- v032, phase 2. Optimistic locking: writers send the version they read and lose if it moved.
-- +goose Up
ALTER TABLE users ADD COLUMN version integer NOT NULL DEFAULT 1;

-- +goose Down
ALTER TABLE users DROP COLUMN version;
