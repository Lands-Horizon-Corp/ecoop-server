-- v011, phase 1. Lifecycle state, as text for now; Phase 2 hardens it into an enum.
-- +goose Up
ALTER TABLE users ADD COLUMN status text NOT NULL DEFAULT 'active';

-- +goose Down
ALTER TABLE users DROP COLUMN status;
