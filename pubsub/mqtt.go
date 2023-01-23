package pubsub

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/mirror520/events/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MqttPubSub interface {
	PubSub
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

func (pubSub *mqttPubSub) Publish(topic string, messages ...*message.Message) error {
	for _, message := range messages {
		payload := []byte(message.Payload)

		token := pubSub.client.Publish(topic, pubSub.cfg.QoS, false, payload)
		token.Wait()

		if err := token.Error(); err != nil {
			return err
		}
	}

	return nil
}

func (pubSub *mqttPubSub) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	messages := make(chan *message.Message)

	token := pubSub.client.Subscribe(topic, pubSub.cfg.QoS, func(c mqtt.Client, m mqtt.Message) {
		msg := message.NewMessage(watermill.NewUUID(), m.Payload())
		msg.Metadata.Set("topic", m.Topic())
	})

	token.Wait()
	if err := token.Error(); err != nil {
		return nil, err
	}

	return messages, nil
}

func (pubSub *mqttPubSub) Close() error {
	pubSub.client.Disconnect(200)
	return nil
}
