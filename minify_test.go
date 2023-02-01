package events

import (
	"context"
	"testing"

	"github.com/go-kit/kit/endpoint"
	"github.com/stretchr/testify/assert"
)

func debugMiddleware(ack chan<- any) endpoint.Endpoint {
	return func(ctx context.Context, request any) (any, error) {
		go func(ack chan<- any) {
			ack <- request
		}(ack)

		return nil, nil
	}
}

func TestMinifyMiddleware(t *testing.T) {
	assert := assert.New(t)

	ack := make(chan any)

	endpoint := debugMiddleware(ack)
	endpoint = MinifyMiddleware("json")(endpoint)

	input := []byte(`{
		"from": "14c5f88a",
		"to": "bb75a980",
		"value": 10,
		"timestamp": "2023-01-22T23:35:00.000+08:00"
	}`)

	req := StoreRequest{
		Topic:   "hello/world",
		Payload: input,
	}

	endpoint(context.Background(), req)
	request := <-ack

	req, ok := request.(StoreRequest)
	if !ok {
		assert.Fail("invalid type")
		return
	}

	output := `{"from":"14c5f88a","to":"bb75a980","value":10,"timestamp":"2023-01-22T23:35:00.000+08:00"}`
	assert.Equal(output, string(req.Payload))
	assert.True(len(req.Payload) <= len(input))
}
