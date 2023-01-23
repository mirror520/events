package events

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/go-kit/kit/endpoint"

	"github.com/mirror520/events/model/event"
)

type StoreRequest struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

func StoreEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (response any, err error) {
		req, ok := request.(StoreRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		e := event.NewEvent(req.Topic, req.Payload)
		if err := svc.Store(e); err != nil {
			return nil, err
		}

		return nil, nil
	}
}
