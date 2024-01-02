package events

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/oklog/ulid/v2"
)

type StoreRequest struct {
	ID      ulid.ULID `json:"id"`
	Topic   string    `json:"topic"`
	Payload Payload   `json:"payload"`
}

func StoreEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (any, error) {
		req, ok := request.(StoreRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		var err error
		if req.ID.Time() == 0 {
			err = svc.Store(req.Topic, req.Payload)
		} else {
			err = svc.Store(req.Topic, req.Payload, req.ID)
		}

		return nil, err
	}
}

type NewIteratorRequest struct {
	Topic string    `json:"topic"`
	Since time.Time `json:"since"`
}

func NewIteratorEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (any, error) {
		req, ok := request.(NewIteratorRequest)
		if !ok {
			return nil, errors.New("invalid request")
		}

		return svc.NewIterator(req.Topic, req.Since)
	}
}

func IteratorEndpoint(svc Service) endpoint.Endpoint {
	return func(ctx context.Context, request any) (any, error) {
		id, ok := request.(string)
		if !ok {
			return nil, errors.New("invalid request")
		}

		return svc.Iterator(id)
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
