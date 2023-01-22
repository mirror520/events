package events

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mirror520/events/model/event"
)

type Endpoint func(ctx context.Context, request any) (response any, err error)

type EventStoreRequest struct {
	Topic   string
	Payload json.RawMessage
}

func EventStoreEndpoint(svc Service) Endpoint {
	return func(ctx context.Context, request any) (response any, err error) {
		req, ok := request.(EventStoreRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		e := event.NewEvent(req.Topic, req.Payload)
		if err := svc.EventStore(e); err != nil {
			return nil, err
		}

		return nil, nil
	}
}
