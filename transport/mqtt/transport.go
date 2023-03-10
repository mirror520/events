package mqtt

import (
	"context"

	"github.com/go-kit/kit/endpoint"

	"github.com/mirror520/events"
	"github.com/mirror520/events/pubsub"
)

func StoreHandler(endpoint endpoint.Endpoint) pubsub.MessageHandler {
	return func(msg pubsub.Message) {
		request := events.StoreRequest{
			Topic:   msg.Topic(),
			Payload: msg.Payload(),
		}

		_, err := endpoint(context.Background(), request)
		if err != nil {
			return
		}
	}
}
