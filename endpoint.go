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

		err := svc.Store(req.Topic, req.Payload)
		return nil, err
	}
}

type IteratorRequest struct {
	Topic string    `json:"topic"`
	Since time.Time `json:"since"`
}

func IteratorEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (any, error) {
		req, ok := request.(IteratorRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		it, err := svc.Iterator(req.Topic, req.Since)
		if err != nil {
			return nil, err
		}

		return it.ID(), nil
	}
}

type FetchFromIteratorRequest struct {
	ID    string
	Batch int
}

func FetchFromIterator(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (any, error) {
		req, ok := request.(FetchFromIteratorRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		return svc.FetchFromIterator(req.Batch, req.ID)
	}
}

func CloseIterator(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (any, error) {
		id, ok := request.(string)
		if !ok {
			return nil, errors.New("invalid request")
		}

		err := svc.CloseIterator(id)
		return nil, err
	}
}
