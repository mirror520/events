package event

import (
	"context"
	"time"
)

type Repository interface {
	Store(e *Event) error
	Iterator(ctx context.Context, ch chan<- *Event, since time.Time) <-chan error
	Close() error
}
