package event

import (
	"time"

	"github.com/oklog/ulid/v2"
)

type Event struct {
	ID      ulid.ULID
	Topic   string
	Payload []byte
}

func NewEvent(topic string, payload []byte) *Event {
	return &Event{
		ID:      ulid.Make(),
		Topic:   topic,
		Payload: payload,
	}
}

func (e *Event) Time() time.Time {
	return ulid.Time(e.ID.Time())
}
