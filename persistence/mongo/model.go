package mongo

import (
	"encoding/json"
	"errors"
	"net/url"
	"time"

	"github.com/oklog/ulid/v2"
	"go.mongodb.org/mongo-driver/bson"

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
	ID      ulid.ULID      `bson:"id"`
	Topic   string         `bson:"topic"`
	Payload events.Payload `bson:"payload"`
}

func NewEvent(e *events.Event) *Event {
	return &Event{
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

func (e *Event) MarshalBSON() ([]byte, error) {
	ms := int64(e.ID.Time())
	ts := time.UnixMilli(ms)

	var payload any

	raw, ok := e.Payload.JSON()
	if !ok {
		payload = e.Payload.Data
	} else {
		json.Unmarshal(raw, &payload)
	}

	output := struct {
		Time    time.Time `bson:"_time"`
		ID      string    `bson:"id"`
		Topic   string    `bson:"topic"`
		Payload any       `bson:"payload"`
	}{
		Time:    ts,
		ID:      e.ID.String(),
		Topic:   e.Topic,
		Payload: payload,
	}

	return bson.Marshal(&output)
}

func (e *Event) UnmarshalBSON(data []byte) error {
	var raw struct {
		Time    time.Time     `bson:"_time"`
		ID      string        `bson:"id"`
		Topic   string        `bson:"topic"`
		Payload bson.RawValue `bson:"payload"`
	}

	if err := bson.Unmarshal(data, &raw); err != nil {
		return err
	}

	id, err := ulid.Parse(raw.ID)
	if err != nil {
		return err
	}
	e.ID = id

	e.Topic = raw.Topic

	switch raw.Payload.Type {
	case bson.TypeDouble:
		e.Payload = events.NewPayload(raw.Payload.Double())

	case bson.TypeString:
		e.Payload = events.NewPayload(raw.Payload.StringValue())

	case bson.TypeEmbeddedDocument:
		bs, err := bson.MarshalExtJSON(raw.Payload.Document(), false, false)
		if err != nil {
			return err
		}

		e.Payload = events.NewPayloadFromJSON(bs)

	case bson.TypeArray:
		var arr []any
		err := raw.Payload.Unmarshal(&arr)
		if err != nil {
			return err
		}

		bs, err := json.Marshal(&arr)
		if err != nil {
			return err
		}

		e.Payload = events.NewPayloadFromJSON(bs)

	case bson.TypeBinary:
		subtype, bs := raw.Payload.Binary()
		if subtype != bson.TypeBinaryGeneric {
			return errors.New("invalid subtype")
		}

		payload, err := events.NewPayloadFromBytes(bs, true)
		if err != nil {
			return err
		}

		e.Payload = payload

	case bson.TypeBoolean:
		e.Payload = events.NewPayload(raw.Payload.Boolean())

	case bson.TypeNull:
		e.Payload = events.NewPayload(nil)

	default:
		return errors.New("invalid type")
	}

	return nil
}
