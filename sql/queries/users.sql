-- name: CreateUser :copyfrom
INSERT INTO users (name)
VALUES ($1);