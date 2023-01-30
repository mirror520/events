package events

import (
	"context"
	"errors"

	"github.com/go-kit/kit/endpoint"
	"go.uber.org/zap"

	"github.com/mirror520/events/pubsub"
)

func MQTTStoreHandler(endpoint endpoint.Endpoint) pubsub.MessageHandler {
	log := zap.L().With(
		zap.String("transport", "mqtt"),
		zap.String("handler", "store"),
	)

	return func(msg pubsub.Message) {
		request := StoreRequest{
			Topic:   msg.Topic(),
			Payload: msg.Payload(),
		}

		_, err := endpoint(context.Background(), request)
		if err != nil {
			if errors.Is(err, ErrReplaying) || errors.Is(err, ErrEmptyPayload) {
				return
			}

			log.Error(err.Error())
			return
		}
	}
}
