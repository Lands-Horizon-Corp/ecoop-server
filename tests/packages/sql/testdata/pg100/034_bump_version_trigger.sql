-- v034, phase 2. The version moves on every update, by the database.
-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION bump_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.version := OLD.version + 1;
  RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER users_bump_version BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION bump_version();
CREATE TRIGGER accounts_bump_version BEFORE UPDATE ON accounts FOR EACH ROW EXECUTE FUNCTION bump_version();

-- +goose Down
DROP TRIGGER accounts_bump_version ON accounts;
DROP TRIGGER users_bump_version ON users;
DROP FUNCTION bump_version();
