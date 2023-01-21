package pubsub

import (
	"context"
	"strconv"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/mirror520/events/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MqttPubSub interface {
	message.Publisher
	message.Subscriber
}

type mqttPubSub struct {
	cfg    *model.MqttConfig
	client mqtt.Client
}

func NewMqttPubSub(cfg *model.MqttConfig) (MqttPubSub, error) {
	opts := mqtt.NewClientOptions().
		SetClientID(cfg.ClientID).
		AddBroker(cfg.Broker).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password)

	client := mqtt.NewClient(opts)

	token := client.Connect()
	token.Wait()

	if err := token.Error(); err != nil {
		return nil, err
	}

	return &mqttPubSub{cfg, client}, nil
}

func (pubsub *mqttPubSub) Publish(topic string, messages ...*message.Message) error {
	for _, message := range messages {
		payload := []byte(message.Payload)

		token := pubsub.client.Publish(topic, pubsub.cfg.QoS, false, payload)
		token.Wait()

		if err := token.Error(); err != nil {
			return err
		}
	}

	return nil
}

func (pubsub *mqttPubSub) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	msg := make(chan *message.Message)

	token := pubsub.client.Subscribe(topic, pubsub.cfg.QoS, func(c mqtt.Client, m mqtt.Message) {
		id := int(m.MessageID())

		msg <- &message.Message{
			UUID:    strconv.Itoa(id),
			Payload: m.Payload(),
		}
	})

	token.Wait()
	if err := token.Error(); err != nil {
		return nil, err
	}

	return msg, nil
}

func (pubsub *mqttPubSub) Close() error {
	pubsub.client.Disconnect(200)
	return nil
}