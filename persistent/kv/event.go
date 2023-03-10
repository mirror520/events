package kv

import (
	"context"
	"encoding/json"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/oklog/ulid/v2"

	"github.com/mirror520/events/model/event"
)

type eventRepository struct {
	db *badger.DB
}

func NewEventRepository() event.Repository {
	repo := new(eventRepository)
	repo.db = DB()
	return repo
}

func (repo *eventRepository) Store(e *event.Event) error {
	key := e.ID.Bytes()
	val, err := json.Marshal(e)
	if err != nil {
		return err
	}

	return repo.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func (repo *eventRepository) Iterator(ctx context.Context, ch chan<- *event.Event, since time.Time) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		err := repo.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			if since.IsZero() {
				it.Rewind()
			} else {
				ms := ulid.Timestamp(since)

				var id ulid.ULID
				err := id.SetTime(ms)
				if err != nil {
					return err
				}

				prefix := id[0:6]

				it.Seek(prefix)
			}

			for ; it.Valid(); it.Next() {
				select {
				case <-ctx.Done():
					return nil

				default:
					item := it.Item()

					err := item.Value(func(val []byte) error {
						var e *event.Event
						if err := json.Unmarshal(val, &e); err != nil {
							return err
						}

						ch <- e

						return nil
					})

					if err != nil {
						return err
					}
				}
			}

			return nil
		})

		errCh <- err // done or fail
	}()

	return errCh
}

func (repo *eventRepository) Close() error {
	return repo.db.Close()
}
