package mongo

import (
	"errors"
	"net/url"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/mirror520/events"
)

type Config struct {
	URI        string
	Database   string
	Collection string
	Duration   time.Duration
}

func parseConfig(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	if !q.Has("db") {
		return nil, errors.New("invalid database")
	}

	db := q.Get("db")

	collection := "events"
	if q.Has("collection") {
		collection = q.Get("collection")
	}

	duration := 10 * time.Second
	if q.Has("duration") {
		durationStr := q.Get("duration")
		dur, err := time.ParseDuration(durationStr)
		if err != nil {
			return nil, err
		}

		duration = dur
	}

	q.Del("db")
	q.Del("collection")
	q.Del("duration")

	u.RawQuery = q.Encode()

	return &Config{
		URI:        u.String(),
		Database:   db,
		Collection: collection,
		Duration:   duration,
	}, nil
}

type Event struct {
	Time    time.Time      `bson:"_time"`
	ID      ulid.ULID      `bson:"id"`
	Topic   string         `bson:"topic"`
	Payload events.Payload `bson:"payload"`
}

func NewEvent(e *events.Event) *Event {
	ms := int64(e.ID.Time())
	ts := time.UnixMilli(ms)

	return &Event{
		Time:    ts,
		ID:      e.ID,
		Topic:   e.Topic,
		Payload: e.Payload,
	}
}

func (e *Event) Event() *events.Event {
	return &events.Event{
		ID:      e.ID,
		Topic:   e.Topic,
		Payload: e.Payload,
	}
}
