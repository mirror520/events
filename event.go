package events

import (
	"encoding/json"
	"time"

	"github.com/oklog/ulid/v2"
)

type Event struct {
	ID      ulid.ULID       `json:"id"`
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

func NewEvent(topic string, payload json.RawMessage, ids ...ulid.ULID) *Event {
	var id ulid.ULID
	if len(ids) > 0 {
		id = ids[0]
	} else {
		id = ulid.Make()
	}

	return &Event{
		ID:      id,
		Topic:   topic,
		Payload: payload,
	}
}

func (e *Event) Time() time.Time {
	return ulid.Time(e.ID.Time())
}
