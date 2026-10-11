-- v074, phase 3. Hash index: equality-only lookups by reference, smaller than a btree for long
-- keys.
-- +goose Up
CREATE INDEX transactions_reference_hash_idx ON transactions USING hash (reference);

-- +goose Down
DROP INDEX transactions_reference_hash_idx;
