package inmem

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/mirror520/events/model/event"
)

type eventRepository struct {
	events []*event.Event
	sync.RWMutex
}

func NewEventRepository() event.Repository {
	repo := new(eventRepository)
	repo.events = make([]*event.Event, 0)
	return repo
}

func (repo *eventRepository) Store(e *event.Event) error {
	repo.Lock()
	defer repo.Unlock()

	events := repo.events
	for i, event := range events {
		if bytes.Compare(event.ID.Bytes(), e.ID.Bytes()) < 1 {
			continue
		}

		events = append(events, nil)
		copy(events[i+1:], events[i:])
		events[i] = e

		repo.events = events
		return nil
	}

	repo.events = append(repo.events, e)
	return nil
}

func (repo *eventRepository) Iterator(ctx context.Context, ch chan<- *event.Event, since time.Time) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		repo.RLock()
		defer repo.RUnlock()

		for _, e := range repo.events {
			select {
			case <-ctx.Done():
				return

			default:
				if !since.IsZero() && e.Time().Before(since) {
					continue
				}

				ch <- e
			}
		}

		errCh <- nil // done
	}()

	return errCh
}

func (repo *eventRepository) Close() error {
	repo.Lock()
	defer repo.Unlock()

	for i := range repo.events {
		repo.events[i] = nil
	}

	repo.events = nil
	return nil
}
