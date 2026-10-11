-- v061, phase 3. BRIN: transactions arrive in time order, so a block-range index is a few pages
-- instead of a btree of every row.
-- +goose Up
CREATE INDEX transactions_created_brin ON transactions USING brin (created_at);
DROP INDEX transactions_created_at_idx;

-- +goose Down
CREATE INDEX transactions_created_at_idx ON transactions (created_at);
DROP INDEX transactions_created_brin;
