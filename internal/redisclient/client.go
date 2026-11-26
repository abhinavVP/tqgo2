package redisclient

import (
	"context"
	"fmt"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type Client struct {
    rdb *redis.Client
}

func New(ctx context.Context, addr, password string) (*Client, error) {
    rdb := redis.NewClient(&redis.Options{
        Addr:         addr,
        Password:     password,
        DB:           0,
        PoolSize:     10,
        DialTimeout:  5 * time.Second,
        ReadTimeout:  3 * time.Second,
        WriteTimeout: 3 * time.Second,
    })

    if err := rdb.Ping(ctx).Err(); err != nil {
        rdb.Close()
        return nil, fmt.Errorf("connect to redis: %w", err)
    }

    return &Client{rdb: rdb}, nil
}

func (c *Client) Close() error {
    return c.rdb.Close()
}

func (c *Client) AquireKey(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
    return c.rdb.SetNX(
        ctx, key, value, ttl,
    ).Result()
}

func (c *Client) ReleaseKey(ctx context.Context, key, value string) (bool, error) {
    args := redis.DelExArgs{
        Mode: "IFEQ",
        MatchValue: value,
    }
    n, err := c.rdb.DelExArgs(
        ctx, key, args,
    ).Result()

    return n==1, err
}

func (c *Client) ReleaseKeyWithValue(ctx context.Context, key string) (string, error) {
    return c.rdb.GetDel(ctx, key).Result()
}