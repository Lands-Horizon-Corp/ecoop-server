-- v002, phase 1. Accounts belong to users through a real foreign key, so orphans are impossible
-- from the start.
-- +goose Up
CREATE TABLE accounts (
  id         bigserial PRIMARY KEY,
  user_id    bigint NOT NULL REFERENCES users (id),
  number     text NOT NULL,
  balance    numeric(14,2) NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE accounts;
