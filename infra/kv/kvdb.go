package kv

import (
	"context"
	"time"
)

type KVDatabase interface {
	Get(ctx context.Context, key string, value any) error
	Set(ctx context.Context, key string, value any) error
	SetEX(ctx context.Context, key string, value any, expiration time.Duration) error
	Del(ctx context.Context, keys ...string) error
	Close() error
}
