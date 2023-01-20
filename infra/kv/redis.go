package kv

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisDatabase interface {
	KVDatabase
}

type redisDatabase struct {
	instance *redis.Client
}

func NewRedisDatabase(addr string) (RedisDatabase, error) {
	opts := &redis.Options{
		Addr: addr,
	}

	instance := redis.NewClient(opts)
	_, err := instance.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}

	return &redisDatabase{instance}, nil
}

func (db *redisDatabase) Get(ctx context.Context, key string, value any) error {
	data, err := db.instance.Get(ctx, key).Bytes()
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &value)
}

func (db *redisDatabase) Set(ctx context.Context, key string, value any) error {
	return db.SetWithTTL(ctx, key, value, 0)
}

func (db *redisDatabase) SetWithTTL(ctx context.Context, key string, value any, ttl time.Duration) error {
	var val []byte
	switch v := value.(type) {
	case []byte:
		val = v

	default:
		buf, err := json.Marshal(value)
		if err != nil {
			return err
		}
		val = buf
	}

	return db.instance.Set(ctx, key, val, ttl).Err()
}

func (db *redisDatabase) Delete(ctx context.Context, keys ...string) error {
	return db.instance.Del(ctx, keys...).Err()
}

func (db *redisDatabase) Close() error {
	return db.instance.Close()
}
