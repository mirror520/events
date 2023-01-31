package events

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-kit/kit/endpoint"
)

type StoreRequest struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

func StoreEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (any, error) {
		req, ok := request.(StoreRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		return svc.Store(req.Topic, req.Payload)
	}
}

type ReplayRequest struct {
	From   time.Time `json:"from"`
	Topics []string  `json:"topics"`
}

func ReplayEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (any, error) {
		req, ok := request.(ReplayRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		err := svc.Replay(req.From, req.Topics...)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}
}
