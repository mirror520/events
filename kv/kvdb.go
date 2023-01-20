package kv

import (
	"context"
	"time"
)

type KVDatabase interface {
	Get(ctx context.Context, key string, value any) error
	Set(ctx context.Context, key string, value any) error
	SetWithTTL(ctx context.Context, key string, value any, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	Close() error
}
