-- v077, phase 3. MILESTONE v075 (Phase 3: Performance & Advanced Indexing). Business rules evolve
-- with the indexes: an overdraft limit replaces 'never negative'. The old check is swapped for one
-- that reads two columns, inside one transaction, so there is no moment without a rule. By now
-- transactions are range-partitioned and every hot path has a partial, covering, expression, GIN,
-- BRIN or hash index shaped for it.
-- +goose Up
ALTER TABLE accounts ADD COLUMN overdraft_limit_cents bigint NOT NULL DEFAULT 0;
ALTER TABLE accounts ADD CONSTRAINT accounts_overdraft_check CHECK (overdraft_limit_cents >= 0);
ALTER TABLE accounts DROP CONSTRAINT accounts_balance_cents_check;
ALTER TABLE accounts ADD CONSTRAINT accounts_balance_within_overdraft CHECK (balance_cents >= -overdraft_limit_cents);

-- +goose Down
ALTER TABLE accounts DROP CONSTRAINT accounts_balance_within_overdraft;
ALTER TABLE accounts ADD CONSTRAINT accounts_balance_cents_check CHECK (balance_cents >= 0);
ALTER TABLE accounts DROP CONSTRAINT accounts_overdraft_check;
ALTER TABLE accounts DROP COLUMN overdraft_limit_cents;
