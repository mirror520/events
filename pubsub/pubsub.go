package pubsub

import (
	"github.com/mirror520/events/model"

	"github.com/ThreeDotsLabs/watermill/message"
)

// TODO: remove watermill
type PubSub interface {
	message.Publisher
	message.Subscriber
}

func Factory(source *model.Source) (PubSub, error) {
	var pubSub PubSub
	var err error

	switch source.Type {
	case model.MQTT_SOURCE:
		pubSub, err = NewMqttPubSub(source.MqttConfig)
	}

	return pubSub, err
}
