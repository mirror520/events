package events

import (
	"context"
	"errors"

	"github.com/go-kit/kit/endpoint"
	"github.com/tdewolff/minify/v2"
	"github.com/tdewolff/minify/v2/json"
	"github.com/tdewolff/minify/v2/xml"

	"github.com/mirror520/events/model"
)

func MinifyMiddleware(format ...string) endpoint.Middleware {
	m := minify.New()
	if len(format) == 0 {
		m.AddFunc("application/json", json.Minify)
	} else {
		for _, f := range format {
			switch f {
			case "json":
				m.AddFunc("application/json", json.Minify)
			case "xml":
				m.AddFunc("application/xml", xml.Minify)
			}
		}
	}

	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {
			req, ok := request.(StoreRequest)
			if !ok {
				return nil, errors.New("invalid request")
			}

			mime, ok := ctx.Value(model.MIME).(string)
			if !ok {
				mime = "application/json"
			}

			payload, err := m.Bytes(mime, req.Payload)
			if err != nil {
				return nil, err
			}

			req.Payload = payload

			return next(ctx, req)
		}
	}
}
