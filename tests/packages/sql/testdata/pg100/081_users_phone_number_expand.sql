-- v081, phase 4. Expand-and-contract rename (phone to phone_number), step 1: add the new column.
-- Catalog-only, and lock_timeout makes the brief lock give up rather than queue behind traffic.
-- +goose Up
SET LOCAL lock_timeout = '5s';
ALTER TABLE users ADD COLUMN phone_number text;

-- +goose Down
ALTER TABLE users DROP COLUMN phone_number;
