package events

import (
	"context"
	"errors"
	"time"
)

var (
	ErrTimeout = errors.New("timeout")
)

type Repository interface {
	Store(e *Event) error
	Iterator(ctx context.Context, since time.Time) (Iterator, error)
	Close() error
}

type Iterator interface {
	ID() string
	Fetch(batch int) ([]*Event, error)
	Close(err error)
	Done() <-chan struct{}
	Err() error
}
