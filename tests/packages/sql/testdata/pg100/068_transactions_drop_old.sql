-- v068, phase 3. Step 4: the old table goes once nothing reads it. Down rebuilds it exactly and
-- copies the rows back.
-- +goose Up
DROP TABLE transactions_old;

-- +goose Down
CREATE TABLE transactions_old (
  id                  bigint NOT NULL DEFAULT nextval('transactions_id_seq'::regclass),
  account_id          bigint NOT NULL,
  kind                transaction_kind NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  description         text,
  reference           text NOT NULL,
  counterparty        text,
  amount_cents        bigint NOT NULL,
  balance_after_cents bigint,
  metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT transactions_old_pkey PRIMARY KEY (id),
  CONSTRAINT transactions_old_account_id_fkey FOREIGN KEY (account_id) REFERENCES accounts (id),
  CONSTRAINT transactions_amount_cents_check CHECK (amount_cents <> 0)
);
CREATE UNIQUE INDEX transactions_old_account_reference_key ON transactions_old (account_id, reference);
CREATE INDEX transactions_old_account_created_cover_idx ON transactions_old (account_id, created_at DESC) INCLUDE (amount_cents);
CREATE INDEX transactions_old_created_brin ON transactions_old USING brin (created_at);
CREATE INDEX transactions_old_metadata_gin ON transactions_old USING gin (metadata jsonb_path_ops);
CREATE INDEX transactions_old_day_idx ON transactions_old (date_trunc('day', created_at AT TIME ZONE 'UTC'));
INSERT INTO transactions_old SELECT * FROM transactions;
