-- v001, phase 1. MILESTONE v001 (Phase 1: Foundations). The first table: a surrogate bigserial
-- key, NOT NULL on what the business cannot do without, and timestamptz from day one so every
-- later time-based feature (partitioning in Phase 3) has a correct, zone-aware column to build on.
-- +goose Up
CREATE TABLE users (
  id         bigserial PRIMARY KEY,
  email      text NOT NULL,
  full_name  text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE users;
