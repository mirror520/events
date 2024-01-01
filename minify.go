package events

import (
	"context"
	"errors"

	"github.com/go-kit/kit/endpoint"
	"github.com/tdewolff/minify/v2"
	"github.com/tdewolff/minify/v2/json"
)

func MinifyMiddleware() endpoint.Middleware {
	m := minify.New()
	m.AddFunc("application/json", json.Minify)

	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request any) (any, error) {
			req, ok := request.(StoreRequest)
			if !ok {
				return nil, errors.New("invalid request")
			}

			raw, ok := req.Payload.JSON()
			if !ok {
				return next(ctx, req)
			}

			payload, err := m.Bytes("application/json", raw)
			if err != nil {
				return nil, err
			}

			req.Payload.SetJSON(payload)

			return next(ctx, req)
		}
	}
}
