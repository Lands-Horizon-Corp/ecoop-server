-- v009, phase 1. A constant default is stored in the catalog (PG11+), so even NOT NULL DEFAULT is
-- instant.
-- +goose Up
ALTER TABLE accounts ADD COLUMN currency char(3) NOT NULL DEFAULT 'PHP';

-- +goose Down
ALTER TABLE accounts DROP COLUMN currency;
