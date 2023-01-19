package kv

import (
	"context"
	"encoding/json"
	"time"

	badger "github.com/dgraph-io/badger/v3"
)

type BadgerDatabase interface {
	KVDatabase
}

type badgerDatabase struct {
	instance *badger.DB
}

func NewBadgerDatabase(path string) (BadgerDatabase, error) {
	opts := badger.DefaultOptions(path).
		WithDir(path + "/meta").
		WithValueDir(path + "/data")

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &badgerDatabase{db}, nil
}

func (db *badgerDatabase) Get(ctx context.Context, key string, value any) error {
	return db.instance.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, value)
		})
	})
}

func (db *badgerDatabase) Set(ctx context.Context, key string, value any) error {
	return db.SetEX(ctx, key, value, 0)
}

func (db *badgerDatabase) SetEX(ctx context.Context, key string, value any, expiration time.Duration) error {
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

	return db.instance.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), val)
		if expiration > 0 {
			e.WithTTL(expiration)
		}

		return txn.SetEntry(e)
	})
}

func (db *badgerDatabase) Del(ctx context.Context, keys ...string) error {
	return db.instance.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			err := txn.Delete([]byte(key))
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *badgerDatabase) Close() error {
	return db.instance.Close()
}
