package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
	"log"
	"os"
	"time"
)

var counts int64

func openDB() (*sql.DB, error) {
	db, err := sql.Open("postgres", "postgres://postgres:password@postgres:5432/lowserver?sslmode=disable")
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func openDBPool() (*pgxpool.Pool, error) {
	dbpool, err := pgxpool.New(context.Background(), "postgres://postgres:password@postgres:5432/lowserver?sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		return nil, err
	}

	err = dbpool.Ping(context.Background())
	if err != nil {
		return nil, err
	}

	return dbpool, nil
}

func ConnectToDB() *pgxpool.Pool {
	for {
		connection, err := openDBPool()
		if err != nil {
			log.Println("Could not connect to database, Postgres is not ready...")
			counts += 1
		} else {
			log.Println("Connected to database...")
			return connection
		}

		if counts > 10 {
			log.Println(err)
			return nil
		}

		log.Println("Waiting for database to become ready...")
		time.Sleep(2 * time.Second)
		continue
	}
}
