package inmem

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mirror520/events"
	"github.com/oklog/ulid/v2"
)

type eventRepository struct {
	events []*events.Event
	sync.RWMutex
}

func NewEventRepository(cfg events.Persistence) (events.Repository, error) {
	repo := new(eventRepository)
	repo.events = make([]*events.Event, 0)
	return repo, nil
}

func (repo *eventRepository) Store(e *events.Event) error {
	repo.Lock()
	defer repo.Unlock()

	events := repo.events
	for i, event := range events {
		if e.ID.Compare(event.ID) > -1 {
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

func (repo *eventRepository) Iterator(ctx context.Context, since time.Time) (events.Iterator, error) {
	ms := ulid.Timestamp(since)

	var last ulid.ULID
	last.SetTime(ms)

	return &iterator{
		id:      "inmem-" + ulid.Make().String(),
		last:    last,
		fetchFn: repo.fetch,
		done:    make(chan struct{}),
	}, nil
}

func (repo *eventRepository) fetch(batch int, last ulid.ULID) ([]*events.Event, error) {
	repo.RLock()
	defer repo.RUnlock()

	start := -1
	end := len(repo.events)

	for i, event := range repo.events {
		if event.ID.Compare(last) > 0 {
			start = i
			break
		}
	}

	if start < 0 {
		return nil, errors.New("event empty")
	}

	if start+batch < end {
		end = start + batch
	}

	return repo.events[start:end], nil
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

type fetch func(batch int, last ulid.ULID) ([]*events.Event, error)

type iterator struct {
	id      string
	last    ulid.ULID
	fetchFn fetch

	done chan struct{}
	err  error
}

func (it *iterator) ID() string {
	return it.id
}

func (it *iterator) Fetch(batch int) ([]*events.Event, error) {
	events, err := it.fetchFn(batch, it.last)
	if err != nil {
		return nil, err
	}

	it.last = events[len(events)-1].ID

	return events, nil
}

func (it *iterator) Close(err error) {
	it.err = err
	it.done <- struct{}{}
}

func (it *iterator) Done() <-chan struct{} {
	return it.done
}

func (it *iterator) Err() error {
	return it.err
}
