package events

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mirror520/events/model/event"
)

type Endpoint func(ctx context.Context, request any) (response any, err error)

type StoreRequest struct {
	Topic   string
	Payload json.RawMessage
}

func StoreEndpoint(svc Service) Endpoint {
	return func(ctx context.Context, request any) (response any, err error) {
		req, ok := request.(StoreRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		// TODO: move to middleware
		payload, err := m.Bytes("application/json", req.Payload)
		if err != nil {
			return nil, err
		}

		e := event.NewEvent(req.Topic, payload)
		if err := svc.Store(e); err != nil {
			return nil, err
		}

		return nil, nil
	}
}
