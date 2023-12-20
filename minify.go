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

type MIME string

const (
	JSON MIME = "application/json"
	XML  MIME = "application/xml"
)

func (m MIME) Type() string {
	return string(m)
}

func MinifyMiddleware(mime ...MIME) endpoint.Middleware {
	m := minify.New()
	if len(mime) == 0 {
		m.AddFunc(JSON.Type(), json.Minify)
	} else {
		for _, t := range mime {
			switch t {
			case JSON:
				m.AddFunc(JSON.Type(), json.Minify)
			case XML:
				m.AddFunc(XML.Type(), xml.Minify)
			}
		}
	}

	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request any) (any, error) {
			req, ok := request.(StoreRequest)
			if !ok {
				return nil, errors.New("invalid request")
			}

			mime := JSON
			mimeStr, ok := ctx.Value(model.MIME).(string)
			if ok {
				mime = MIME(mimeStr)
			}

			payload, err := m.Bytes(mime.Type(), req.Payload)
			if err != nil {
				return nil, err
			}

			req.Payload = payload

			return next(ctx, req)
		}
	}
}
