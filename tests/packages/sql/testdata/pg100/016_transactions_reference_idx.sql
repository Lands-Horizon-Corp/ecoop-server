-- v016, phase 1. Reconciliation looks transactions up by reference.
-- +goose Up
CREATE INDEX transactions_reference_idx ON transactions (reference);

-- +goose Down
DROP INDEX transactions_reference_idx;
