-- v065, phase 3. Range partitioning, step 1 of 4 (online repartitioning). A partitioned twin is
-- built beside the live table, with its indexes, and a trigger forwards every write to it from now
-- on. The primary key must include the partition key, so (account_id, reference) uniqueness
-- becomes per partition: a deliberate trade-off, recorded here. Locks are taken in the
-- application's order (transactions, then accounts): installing the trigger first and the foreign
-- key second, or traffic deadlocks with it.
-- +goose Up
SET LOCAL lock_timeout = '5s';
LOCK TABLE transactions IN SHARE ROW EXCLUSIVE MODE;
-- +goose StatementBegin
CREATE FUNCTION transactions_forward() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    DELETE FROM transactions_p WHERE id = OLD.id AND created_at = OLD.created_at;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO transactions_p SELECT NEW.*;
  END IF;
  RETURN NULL;
END $$;
-- +goose StatementEnd
CREATE TRIGGER transactions_forward AFTER INSERT OR UPDATE OR DELETE ON transactions FOR EACH ROW EXECUTE FUNCTION transactions_forward();
CREATE TABLE transactions_p (LIKE transactions INCLUDING DEFAULTS INCLUDING CONSTRAINTS) PARTITION BY RANGE (created_at);
ALTER TABLE transactions_p ADD CONSTRAINT transactions_p_pkey PRIMARY KEY (id, created_at);
ALTER TABLE transactions_p ADD CONSTRAINT transactions_p_account_id_fkey FOREIGN KEY (account_id) REFERENCES accounts (id);
CREATE TABLE transactions_y2023 PARTITION OF transactions_p FOR VALUES FROM ('2023-01-01 00:00:00+00') TO ('2024-01-01 00:00:00+00');
CREATE TABLE transactions_y2024 PARTITION OF transactions_p FOR VALUES FROM ('2024-01-01 00:00:00+00') TO ('2025-01-01 00:00:00+00');
CREATE TABLE transactions_y2025 PARTITION OF transactions_p FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');
CREATE TABLE transactions_y2026 PARTITION OF transactions_p FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');
CREATE TABLE transactions_y2027 PARTITION OF transactions_p FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2028-01-01 00:00:00+00');
CREATE TABLE transactions_default PARTITION OF transactions_p DEFAULT;
CREATE UNIQUE INDEX transactions_p_account_reference_key ON transactions_p (account_id, reference, created_at);
CREATE INDEX transactions_p_account_created_cover_idx ON transactions_p (account_id, created_at DESC) INCLUDE (amount_cents);
CREATE INDEX transactions_p_created_brin ON transactions_p USING brin (created_at);
CREATE INDEX transactions_p_metadata_gin ON transactions_p USING gin (metadata jsonb_path_ops);
CREATE INDEX transactions_p_day_idx ON transactions_p (date_trunc('day', created_at AT TIME ZONE 'UTC'));

-- +goose Down
DROP TRIGGER transactions_forward ON transactions;
DROP FUNCTION transactions_forward();
DROP TABLE transactions_p;
