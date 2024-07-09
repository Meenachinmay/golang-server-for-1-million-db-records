-- name: CreateUser :one
INSERT INTO users (name)
VALUES ($1)
RETURNING *;