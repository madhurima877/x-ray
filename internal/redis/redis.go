// Package redis connects to Redis.
package redis

import (
	"context"
	"os"

	goredis "github.com/redis/go-redis/v9"
)

// Connect opens a Redis client and checks it with a ping.
func Connect(ctx context.Context) (*goredis.Client, error) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, err
	}
	return rdb, nil
}
