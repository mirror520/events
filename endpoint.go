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
	return func(ctx context.Context, request any) (response any, err error) {
		req, ok := request.(StoreRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		err = svc.Store(req.Topic, req.Payload)
		return
	}
}

type PlaybackRequest struct {
	From   time.Time `json:"from"`
	Topics []string  `json:"topics"`
}

func PlaybackEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (response any, err error) {
		req, ok := request.(PlaybackRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		err = svc.Playback(req.From, req.Topics...)
		return
	}
}
