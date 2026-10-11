-- v050, phase 2. MILESTONE v050 (end of Phase 2). The schema is now normalized and self-defending:
-- enums, cents, uniques, audit and version columns maintained by triggers, and cross-column rules
-- like this one (a closed account must say when it closed). From here on the data is large, so
-- Phase 3 is about reading it fast and Phase 4 about changing it without stopping the bank.
-- +goose Up
ALTER TABLE accounts ADD COLUMN closed_at timestamptz;
ALTER TABLE accounts ADD CONSTRAINT accounts_closed_check CHECK (status <> 'closed' OR closed_at IS NOT NULL);

-- +goose Down
ALTER TABLE accounts DROP CONSTRAINT accounts_closed_check;
ALTER TABLE accounts DROP COLUMN closed_at;
