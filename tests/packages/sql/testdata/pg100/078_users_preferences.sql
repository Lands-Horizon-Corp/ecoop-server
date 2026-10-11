-- v078, phase 3. User preferences as JSONB, with a default-opclass GIN for key-existence queries.
-- +goose Up
ALTER TABLE users ADD COLUMN preferences jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE INDEX users_preferences_gin ON users USING gin (preferences);

-- +goose Down
DROP INDEX users_preferences_gin;
ALTER TABLE users DROP COLUMN preferences;
