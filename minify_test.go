package events

import (
	"context"
	"encoding/json"
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
	endpoint = MinifyMiddleware()(endpoint)

	input := []byte(`{
		"message": "Hello World",
		"timestamp": "2023-01-22T23:35:00.000+08:00"
	}`)

	req := StoreRequest{
		Topic: "hello/world",
		Payload: Payload{
			Data: json.RawMessage(input),
			Type: JSON,
		},
	}

	endpoint(context.Background(), req)
	request := <-ack

	req, ok := request.(StoreRequest)
	if !ok {
		assert.Fail("invalid type")
		return
	}

	expected := `{"message":"Hello World","timestamp":"2023-01-22T23:35:00.000+08:00"}`

	actual, ok := req.Payload.JSON()
	assert.True(ok)
	assert.Equal(expected, string(actual))
	assert.True(len(actual) <= len(input))
}
