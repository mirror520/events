package event

import "time"

type Repository interface {
	Store(e *Event) error
	Iterator(ch chan<- *Event, from time.Time) error
}
