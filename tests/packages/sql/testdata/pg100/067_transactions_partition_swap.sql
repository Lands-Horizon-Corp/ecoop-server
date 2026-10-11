-- v067, phase 3. Step 3: the swap, in one short transaction. Under the lock the twin is reconciled
-- with the original (any row the copy raced with is fixed), then the names trade places and the id
-- sequence changes owner. The lock lasts for the reconcile, not for the copy, which already
-- happened.
-- +goose Up
SET LOCAL lock_timeout = '5s';
LOCK TABLE transactions IN ACCESS EXCLUSIVE MODE;
DELETE FROM transactions_p p WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.id = p.id AND t::text = p::text);
INSERT INTO transactions_p SELECT * FROM transactions t WHERE NOT EXISTS (SELECT 1 FROM transactions_p p WHERE p.id = t.id);
DROP TRIGGER transactions_forward ON transactions;
DROP FUNCTION transactions_forward();
ALTER TABLE transactions RENAME TO transactions_old;
ALTER TABLE transactions_old RENAME CONSTRAINT transactions_pkey TO transactions_old_pkey;
ALTER TABLE transactions_old RENAME CONSTRAINT transactions_account_id_fkey TO transactions_old_account_id_fkey;
ALTER INDEX transactions_account_reference_key RENAME TO transactions_old_account_reference_key;
ALTER INDEX transactions_account_created_cover_idx RENAME TO transactions_old_account_created_cover_idx;
ALTER INDEX transactions_created_brin RENAME TO transactions_old_created_brin;
ALTER INDEX transactions_metadata_gin RENAME TO transactions_old_metadata_gin;
ALTER INDEX transactions_day_idx RENAME TO transactions_old_day_idx;
ALTER TABLE transactions_p RENAME TO transactions;
ALTER TABLE transactions RENAME CONSTRAINT transactions_p_pkey TO transactions_pkey;
ALTER TABLE transactions RENAME CONSTRAINT transactions_p_account_id_fkey TO transactions_account_id_fkey;
ALTER INDEX transactions_p_account_reference_key RENAME TO transactions_account_reference_key;
ALTER INDEX transactions_p_account_created_cover_idx RENAME TO transactions_account_created_cover_idx;
ALTER INDEX transactions_p_created_brin RENAME TO transactions_created_brin;
ALTER INDEX transactions_p_metadata_gin RENAME TO transactions_metadata_gin;
ALTER INDEX transactions_p_day_idx RENAME TO transactions_day_idx;
ALTER SEQUENCE transactions_id_seq OWNED BY transactions.id;

-- +goose Down
LOCK TABLE transactions IN ACCESS EXCLUSIVE MODE;
DELETE FROM transactions_old o WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.id = o.id AND t::text = o::text);
INSERT INTO transactions_old SELECT * FROM transactions t WHERE NOT EXISTS (SELECT 1 FROM transactions_old o WHERE o.id = t.id);
ALTER SEQUENCE transactions_id_seq OWNED BY transactions_old.id;
ALTER INDEX transactions_account_reference_key RENAME TO transactions_p_account_reference_key;
ALTER INDEX transactions_account_created_cover_idx RENAME TO transactions_p_account_created_cover_idx;
ALTER INDEX transactions_created_brin RENAME TO transactions_p_created_brin;
ALTER INDEX transactions_metadata_gin RENAME TO transactions_p_metadata_gin;
ALTER INDEX transactions_day_idx RENAME TO transactions_p_day_idx;
ALTER TABLE transactions RENAME CONSTRAINT transactions_account_id_fkey TO transactions_p_account_id_fkey;
ALTER TABLE transactions RENAME CONSTRAINT transactions_pkey TO transactions_p_pkey;
ALTER TABLE transactions RENAME TO transactions_p;
ALTER INDEX transactions_old_account_reference_key RENAME TO transactions_account_reference_key;
ALTER INDEX transactions_old_account_created_cover_idx RENAME TO transactions_account_created_cover_idx;
ALTER INDEX transactions_old_created_brin RENAME TO transactions_created_brin;
ALTER INDEX transactions_old_metadata_gin RENAME TO transactions_metadata_gin;
ALTER INDEX transactions_old_day_idx RENAME TO transactions_day_idx;
ALTER TABLE transactions_old RENAME CONSTRAINT transactions_old_account_id_fkey TO transactions_account_id_fkey;
ALTER TABLE transactions_old RENAME CONSTRAINT transactions_old_pkey TO transactions_pkey;
ALTER TABLE transactions_old RENAME TO transactions;
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
