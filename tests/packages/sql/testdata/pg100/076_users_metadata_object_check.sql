-- v076, phase 3. JSONB documents must be objects, not arrays or scalars.
-- +goose Up
ALTER TABLE users ADD CONSTRAINT users_metadata_object_check CHECK (jsonb_typeof(metadata) = 'object');

-- +goose Down
ALTER TABLE users DROP CONSTRAINT users_metadata_object_check;
