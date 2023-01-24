package pubsub

import "github.com/mirror520/events/model"

type Message interface {
	Topic() string
	Payload() []byte
}

type message struct {
	topic   string
	payload []byte
}

func NewMessage(topic string, payload []byte) Message {
	return &message{
		topic:   topic,
		payload: payload,
	}
}

func (msg *message) Topic() string {
	return msg.topic
}

func (msg *message) Payload() []byte {
	return msg.payload
}

type MessageHandler func(Message)

type PubSub interface {
	Publish(topic string, msg Message) error
	Subscribe(topic string, callback MessageHandler) error
	Close() error
}

func Factory(transport *model.Transport) (PubSub, error) {
	var pubSub PubSub
	var err error

	switch transport.Type {
	case model.MQTT_SOURCE:
		pubSub, err = NewMqttPubSub(transport.MqttConfig)
	}

	return pubSub, err
}
