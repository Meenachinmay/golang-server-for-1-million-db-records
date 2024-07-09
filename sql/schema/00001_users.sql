-- +goose Up
CREATE TABLE "users" (
                         "id" bigserial PRIMARY KEY,
                         "name" varchar not null
);

-- +goose Down
DROP TABLE IF EXISTS users;