-- v008, phase 1. A nullable column is a catalog-only change: instant at any size.
-- +goose Up
ALTER TABLE users ADD COLUMN phone text;

-- +goose Down
ALTER TABLE users DROP COLUMN phone;
