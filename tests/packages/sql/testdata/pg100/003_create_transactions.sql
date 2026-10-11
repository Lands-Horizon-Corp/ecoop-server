-- v003, phase 1. Transactions belong to accounts. Money starts as numeric(14,2); Phase 2 moves it
-- to integer cents.
-- +goose Up
CREATE TABLE transactions (
  id         bigserial PRIMARY KEY,
  account_id bigint NOT NULL REFERENCES accounts (id),
  amount     numeric(14,2) NOT NULL,
  kind       text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE transactions;
