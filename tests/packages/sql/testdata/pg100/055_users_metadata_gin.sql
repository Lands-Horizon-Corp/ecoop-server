-- v055, phase 3. GIN with jsonb_path_ops: compact and fast for @> containment queries.
-- +goose Up
CREATE INDEX users_metadata_gin ON users USING gin (metadata jsonb_path_ops);

-- +goose Down
DROP INDEX users_metadata_gin;
