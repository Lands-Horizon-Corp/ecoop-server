-- v058, phase 3. JSONB on accounts, with its GIN index.
-- +goose Up
ALTER TABLE accounts ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE INDEX accounts_metadata_gin ON accounts USING gin (metadata jsonb_path_ops);

-- +goose Down
DROP INDEX accounts_metadata_gin;
ALTER TABLE accounts DROP COLUMN metadata;
