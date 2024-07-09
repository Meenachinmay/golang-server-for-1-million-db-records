up_build:
	docker-compose down && docker-compose up --build -d

down:
	docker compose down

migrate:
	cd sql && cd schema && goose postgres "postgres://postgres:password@localhost:5432/lowserver?sslmode=disable" up

migratedown:
	cd sql && cd schema && goose postgres "postgres://postgres:password@localhost:5432/lowserver?sslmode=disable" down
