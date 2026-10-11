-- v079, phase 3. Partial expression index on a JSONB key, only where the key exists.
-- +goose Up
CREATE INDEX transactions_channel_idx ON transactions ((metadata ->> 'channel')) WHERE metadata ? 'channel';

-- +goose Down
DROP INDEX transactions_channel_idx;
