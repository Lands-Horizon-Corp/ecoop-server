-- v013, phase 1. Account lifecycle state.
-- +goose Up
ALTER TABLE accounts ADD COLUMN status text NOT NULL DEFAULT 'open';

-- +goose Down
ALTER TABLE accounts DROP COLUMN status;
