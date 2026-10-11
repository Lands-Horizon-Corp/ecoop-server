-- v015, phase 1. External reference from the payment rail; nullable until Phase 2 backfills it.
-- +goose Up
ALTER TABLE transactions ADD COLUMN reference text;

-- +goose Down
ALTER TABLE transactions DROP COLUMN reference;
