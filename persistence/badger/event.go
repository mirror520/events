package badger

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/oklog/ulid/v2"

	"github.com/mirror520/events"
)

type eventRepository struct {
	db *badger.DB
}

func NewEventRepository(cfg events.Persistence) (events.Repository, error) {
	opts := badger.DefaultOptions(cfg.DSN)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &eventRepository{db}, nil
}

func (repo *eventRepository) Store(e *events.Event) error {
	key := e.ID.Bytes()
	val, err := json.Marshal(e)
	if err != nil {
		return err
	}

	return repo.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func (repo *eventRepository) Iterator(ctx context.Context, since time.Time) (events.Iterator, error) {
	var (
		prefetchSize = 10
		ch           = make(chan *events.Event, prefetchSize*2) // buffer + prefetch
		errCh        = make(chan error)
	)

	ctx, cancel := context.WithCancelCause(ctx)
	go func(ctx context.Context, ch chan<- *events.Event, errCh chan<- error) {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = prefetchSize

		var last ulid.ULID
		last.SetTime(ulid.Timestamp(since))

		ticker := time.NewTicker(500 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				errCh <- nil
				return

			case <-ticker.C:
				err := repo.db.View(func(txn *badger.Txn) error {
					it := txn.NewIterator(opts)
					defer it.Close()

					for it.Seek(last.Bytes()); it.Valid(); it.Next() {
						item := it.Item()

						if bytes.Equal(item.Key(), last.Bytes()) {
							continue
						}

						err := item.Value(func(val []byte) error {
							var e *events.Event
							if err := json.Unmarshal(val, &e); err != nil {
								return err
							}

							ch <- e

							last = e.ID
							return nil
						})

						if err != nil {
							return err
						}
					}

					return nil
				})

				if err != nil {
					errCh <- err
					return
				}
			}
		}
	}(ctx, ch, errCh)

	it := &iterator{
		id:      "badger-" + ulid.Make().String(),
		timeout: 200 * time.Millisecond,
		ch:      ch,
		errCh:   errCh,
		ctx:     ctx,
		cancel:  cancel,
	}

	go it.handle(ctx, errCh)

	return it, nil
}

func (repo *eventRepository) Close() error {
	return repo.db.Close()
}

type iterator struct {
	id      string
	timeout time.Duration

	ch    <-chan *events.Event
	errCh <-chan error

	ctx    context.Context
	cancel context.CancelCauseFunc
}

func (it *iterator) ID() string {
	return it.id
}

func (it *iterator) Fetch(batch int) ([]*events.Event, error) {
	es := make([]*events.Event, 0)
	after := time.After(it.timeout)
	for {
		select {
		case e := <-it.ch:
			es = append(es, e)
			if len(es) == batch {
				return es, nil
			}

		case <-after:
			if len(es) == 0 {
				return nil, events.ErrTimeout
			}

			return es, nil
		}
	}
}

func (it *iterator) Close(err error) {
	if it.cancel != nil {
		it.cancel(err)
	}

	it.cancel = nil
}

func (it *iterator) Done() <-chan struct{} {
	return it.ctx.Done()
}

func (it *iterator) Err() error {
	return it.ctx.Err()
}

func (it *iterator) handle(ctx context.Context, errCh <-chan error) {
	for {
		select {
		case <-ctx.Done():
			return

		case err := <-errCh:
			it.Close(err)
			return
		}
	}
}
