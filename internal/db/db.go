// Package db connects to Postgres.
package db

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a Postgres pool and checks it with a ping.
func Connect(ctx context.Context) (*pgxpool.Pool, error) {
	url := os.Getenv("POSTGRES_URL")
	if url == "" {
		url = "postgres://xray:xray@localhost:5432/xray?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
