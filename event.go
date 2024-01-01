package events

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/oklog/ulid/v2"
)

type Event struct {
	ID      ulid.ULID `json:"id"`
	Topic   string    `json:"topic"`
	Payload Payload   `json:"payload"`
}

func NewEvent(topic string, payload Payload, ids ...ulid.ULID) *Event {
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

type DataType int

const (
	Any DataType = iota
	JSON
	Bytes
)

type Payload struct {
	Data any
	Type DataType
}

func NewPayload(data any) Payload {
	return Payload{Data: data}
}

func NewPayloadFromJSON(data json.RawMessage) Payload {
	return Payload{Data: data, Type: JSON}
}

func NewPayloadFromBytes(data []byte, raw ...bool) (p Payload, err error) {
	if len(raw) > 0 && raw[0] {
		p.SetBytes(data)
		return
	}

	err = json.Unmarshal(data, &p)
	return
}

func (p *Payload) SetJSON(data json.RawMessage) {
	p.Data = data
	p.Type = JSON
}

func (p *Payload) JSON() (json.RawMessage, bool) {
	if p.Type != JSON {
		return nil, false
	}

	raw, ok := p.Data.(json.RawMessage)
	return raw, ok
}

func (p *Payload) SetBytes(data []byte) {
	p.Data = data
	p.Type = Bytes
}

func (p *Payload) SetData(data any) {
	p.Data = data
	p.Type = Any
}

func (p *Payload) UnmarshalJSON(data []byte) error {
	var payload any
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	switch val := payload.(type) {
	case map[string]any:
		encodedData, ok := val["$binary"].(string)
		if !ok {
			// object
			p.SetJSON(data)
		} else {
			// bytes
			bs, err := base64.StdEncoding.DecodeString(encodedData)
			if err != nil {
				return err
			}

			p.SetBytes(bs)
		}

	case []any: // array
		p.SetJSON(data)
	case float64, string, bool:
		p.SetData(val)
	default:
		p.Data = val
	}

	return nil
}

func (p *Payload) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.Data)
}
