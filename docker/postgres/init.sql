-- Runs once, on the first start of an empty Postgres volume.
-- Sample table used by the GET /users/:id example in main.go.
CREATE TABLE IF NOT EXISTS users (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO users (name) VALUES ('Juan Dela Cruz'), ('Maria Clara');
