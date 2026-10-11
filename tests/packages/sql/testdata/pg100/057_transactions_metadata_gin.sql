-- v057, phase 3. GIN on transaction metadata.
-- +goose Up
CREATE INDEX transactions_metadata_gin ON transactions USING gin (metadata jsonb_path_ops);

-- +goose Down
DROP INDEX transactions_metadata_gin;
