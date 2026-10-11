-- v084, phase 4. Step 4: index the new column without blocking writes (CONCURRENTLY, outside a
-- transaction).
-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY users_phone_number_idx ON users (phone_number);

-- +goose Down
DROP INDEX CONCURRENTLY users_phone_number_idx;
